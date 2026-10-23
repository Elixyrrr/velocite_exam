# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — raw → curated : stations et abonnés
# MAGIC - Abonnés : **minimisation** (nom, e-mail supprimés) + **pseudonymisation** de l'ID (R3, A7)
# MAGIC - MERGE idempotent : relancer = même résultat (A6)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# ---------------- STATIONS (copie complète quotidienne, petite table de référence)
p = raw_path("sql/stations", f"ingest_date={RUN_DATE}")
if not path_exists(p):
    raise Exception(f"Aucune donnée stations dans {p} : l'ingestion SQL a échoué ?")

raw_st = spark.read.parquet(p)
lus = raw_st.count()
st = raw_st.select(
    F.col("station_id").cast("int"),
    F.trim("nom").alias("nom"),
    F.col("capacite").cast("int"),
)
dedup = latest_per_key(st, ["station_id"], "capacite")   # clés NULL conservées -> quarantaine
rules = [
    ("station_id_present", "completude", F.col("station_id").isNotNull()),
    ("capacite_positive", "validite", F.col("capacite") > 0),
]
dq_report("stations", dedup, rules)
ok, ko = apply_rules(dedup, rules)
nq = to_quarantine("stations", ko)

(DeltaTable.forName(spark, "curated.stations").alias("t")
 .merge(ok.withColumn("_updated_at", F.current_timestamp()).alias("s"), "t.station_id = s.station_id")
 .whenMatchedUpdateAll(condition="NOT (t.nom <=> s.nom) OR NOT (t.capacite <=> s.capacite)")
 .whenNotMatchedInsertAll()
 .execute())
reconcile("stations", lus, lus - dedup.count(), nq, ok.count())

# COMMAND ----------

# ---------------- ABONNÉS (incrémental watermark)
p = raw_path("sql/abonnes", f"ingest_date={RUN_DATE}")
if not path_exists(p):
    print("Pas de nouveaux abonnés pour ce run.")
    dbutils.notebook.exit("no_data")

raw_ab = spark.read.parquet(p)
lus = raw_ab.count()

# Pseudonymisation AVANT tout le reste : rien d'identifiant ne sort de cette cellule
ab = raw_ab.select(
    pseudo(F.col("abonne_id")).alias("abonne_pseudo"),
    F.col("abonne_id").isNotNull().alias("_id_present"),
    F.initcap(F.trim("ville")).alias("ville"),
    F.col("modifie_le").cast("timestamp"),
)
dq_uniqueness("abonnes", lus, ab.select("abonne_pseudo").distinct().count())
dedup = latest_per_key(ab, ["abonne_pseudo"], "modifie_le")

rules = [
    ("abonne_id_present", "completude", F.col("_id_present")),
    ("modifie_le_present", "completude", F.col("modifie_le").isNotNull()),
    ("ville_connue", "validite", F.col("ville").isin("Paris", "Lyon", "Lille", "Nantes")),
]
dq_report("abonnes", dedup, rules)
ok, ko = apply_rules(dedup, rules)
nq = to_quarantine("abonnes", ko.drop("_id_present"))

(DeltaTable.forName(spark, "curated.abonnes").alias("t")
 .merge(ok.drop("_id_present").withColumn("_updated_at", F.current_timestamp()).alias("s"),
        "t.abonne_pseudo = s.abonne_pseudo")
 .whenMatchedUpdateAll(condition="s.modifie_le > t.modifie_le")
 .whenNotMatchedInsertAll()
 .execute())
reconcile("abonnes", lus, lus - dedup.count(), nq, ok.count())
