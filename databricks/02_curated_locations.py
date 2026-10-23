# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — raw → curated : locations
# MAGIC | Étape | Anomalie corrigée |
# MAGIC |---|---|
# MAGIC | Lecture du seul dossier `ingest_date` du run (incrémental + lookback 7 j) | A1, R2 |
# MAGIC | Typage explicite, colonnes inconnues conservées | A4, R5 |
# MAGIC | Pseudonymisation abonne_id | R3, A7 |
# MAGIC | Dédoublonnage (dernière version par `location_id`) | A6 |
# MAGIC | Contrôles qualité → quarantaine avec motif | A4 |
# MAGIC | `event_date` = date de **début** | A2 |
# MAGIC | MERGE idempotent | A6, R2 |
# MAGIC | OPTIMIZE + ZORDER abonne_pseudo | A3, A5 |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

p = raw_path("sql/locations", f"ingest_date={RUN_DATE}")
if not path_exists(p):
    raise Exception(f"Aucune donnée locations dans {p}")

raw_loc = spark.read.option("mergeSchema", "true").parquet(p)   # plusieurs run_id possibles (rejeu)
lus = raw_loc.count()

KNOWN = {"location_id", "abonne_id", "station_depart", "station_arrivee", "debut", "fin", "montant", "modifie_le"}
extra_cols = [c for c in raw_loc.columns if c not in KNOWN]   # R5 : nouvelles colonnes source
if extra_cols:
    print(f"[R5] Nouvelles colonnes détectées et propagées : {extra_cols}")

loc = raw_loc.select(
    F.expr("try_cast(location_id AS INT)").alias("location_id"),
    pseudo(F.col("abonne_id")).alias("abonne_pseudo"),
    F.expr("try_cast(station_depart AS INT)").alias("station_depart"),
    F.expr("try_cast(station_arrivee AS INT)").alias("station_arrivee"),
    F.expr("try_cast(debut AS TIMESTAMP)").alias("debut"),
    F.expr("try_cast(fin AS TIMESTAMP)").alias("fin"),
    F.expr("try_cast(montant AS DECIMAL(6,2))").alias("montant"),
    F.expr("try_cast(modifie_le AS TIMESTAMP)").alias("modifie_le"),
    *[F.col(c) for c in extra_cols],
)

# Dédoublonnage : la fenêtre de lookback relit volontairement des lignes déjà vues ; on garde la plus récente
dq_uniqueness("locations", lus, loc.select("location_id").distinct().count())
loc = latest_per_key(loc, ["location_id"], "modifie_le")
apres_dedup = loc.count()

# Intégrité référentielle : stations connues
stations = spark.table("curated.stations").select(F.col("station_id").alias("_st"))
loc = (loc.join(F.broadcast(stations.withColumnRenamed("_st", "_dep")), F.col("station_depart") == F.col("_dep"), "left")
          .join(F.broadcast(stations.withColumnRenamed("_st", "_arr")), F.col("station_arrivee") == F.col("_arr"), "left"))

rules = [
    ("location_id_present", "completude", F.col("location_id").isNotNull()),
    ("abonne_present",      "completude", F.col("abonne_pseudo").isNotNull()),
    ("debut_present",       "completude", F.col("debut").isNotNull()),
    ("fin_presente",        "completude", F.col("fin").isNotNull()),
    ("modifie_le_present",  "completude", F.col("modifie_le").isNotNull()),
    ("fin_apres_debut",     "validite",   F.col("fin") >= F.col("debut")),
    ("montant_positif",     "validite",   F.col("montant") > 0),
    ("duree_max_24h",       "validite",   (F.unix_timestamp("fin") - F.unix_timestamp("debut")) <= 86400),
    ("station_depart_connue",  "coherence", F.col("_dep").isNotNull()),
    ("station_arrivee_connue", "coherence", F.col("_arr").isNotNull()),
]
dq_report("locations", loc, rules, seuils={"montant_positif": 0.99, "fin_presente": 0.98})
ok, ko = apply_rules(loc, rules)
nq = to_quarantine("locations", ko.drop("_dep", "_arr"))

ok = (ok.drop("_dep", "_arr")
        .withColumn("duree_min", (F.unix_timestamp("fin") - F.unix_timestamp("debut")) / 60)
        .withColumn("event_date", F.to_date("debut"))                    # A2 : date d'ÉVÉNEMENT
        .withColumn("event_month", F.date_format("debut", "yyyy-MM"))
        .withColumn("_ingest_date", F.lit(RUN_DATE).cast("date"))
        .withColumn("_updated_at", F.current_timestamp()))
n_merge = ok.count()

# COMMAND ----------

# MERGE idempotent : on ne met à jour que si la version source est plus récente.
# Relancer le même run_date => 0 ligne modifiée (preuve A6 : voir DESCRIBE HISTORY).
target = DeltaTable.forName(spark, "curated.locations")
(target.alias("t")
 .merge(ok.alias("s"), "t.location_id = s.location_id")
 .withSchemaEvolution()                       # R5 : nouvelle colonne source => ajoutée à la cible
 .whenMatchedUpdateAll(condition="s.modifie_le > t.modifie_le")
 .whenNotMatchedInsertAll()
 .execute())

metrics = target.history(1).select("operationMetrics").first()[0]
print({k: metrics.get(k) for k in ["numTargetRowsInserted", "numTargetRowsUpdated", "numSourceRows"]})

reconcile("locations", lus, lus - apres_dedup, nq, n_merge)

# COMMAND ----------

# A3 + A5 : compaction des petits fichiers et co-localisation par abonné (filtres par abonné rapides).
# Seulement sur les partitions touchées, pour maîtriser le coût (R6).
mois = [r[0] for r in ok.select("event_month").distinct().collect()]
if mois:
    liste = ",".join(f"'{m}'" for m in mois)
    spark.sql(f"OPTIMIZE curated.locations WHERE event_month IN ({liste}) ZORDER BY (abonne_pseudo)")
