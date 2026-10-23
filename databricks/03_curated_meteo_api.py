# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — raw → curated : météo (CSV) et disponibilité stations (API)
# MAGIC Pièges traités : fichiers manquants, **colonnes qui changent** (R5), valeurs non typables (A4),
# MAGIC pages JSON, **données tardives** (event time ≠ ingestion time).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# ---------------- MÉTÉO
REQUIRED = ["date", "ville", "temp_min", "temp_max", "precip_mm"]
p = raw_path("meteo", f"ingest_date={RUN_DATE}")

files = [f.path for f in dbutils.fs.ls(p) if f.path.endswith(".csv")] if path_exists(p) else []
if not files:
    print("Aucun CSV météo pour ce run (l'alerte ADF a déjà signalé le fichier manquant). On continue : R1 prime.")
else:
    # Lecture fichier par fichier : un header différent d'un fichier à l'autre ne décale PAS les colonnes
    dfs = []
    for f in files:
        d = spark.read.option("header", "true").option("inferSchema", "false").csv(f)
        missing = [c for c in REQUIRED if c not in d.columns]
        if missing:
            raise Exception(f"[R5] Colonne(s) obligatoire(s) supprimée(s) dans {f} : {missing} — changement cassant, à valider avec le fournisseur")
        dfs.append(d.withColumn("_source_file", F.lit(f.split("/")[-1])))
    raw_m = dfs[0]
    for d in dfs[1:]:
        raw_m = raw_m.unionByName(d, allowMissingColumns=True)

    lus = raw_m.count()
    extra = [c for c in raw_m.columns if c not in REQUIRED + ["_source_file"]]
    if extra:
        print(f"[R5] Colonnes additionnelles acceptées : {extra}")

    m = raw_m.select(
        F.expr("try_cast(date AS DATE)").alias("date"),
        F.initcap(F.trim("ville")).alias("ville"),
        F.expr("try_cast(temp_min AS DOUBLE)").alias("temp_min"),
        F.expr("try_cast(temp_max AS DOUBLE)").alias("temp_max"),
        F.expr("try_cast(precip_mm AS DOUBLE)").alias("precip_mm"),
        *[F.expr(f"try_cast(`{c}` AS DOUBLE)").alias(c) for c in extra],   # nouvelles mesures numériques
        "_source_file",
        # garde la valeur brute pour la quarantaine (diagnostic du typage)
        F.to_json(F.struct(*[F.col(c) for c in REQUIRED])).alias("_brut"),
    )
    m = latest_per_key(m, ["date", "ville"], "_source_file")
    apres = m.count()
    rules = [
        ("date_valide",      "validite",   F.col("date").isNotNull()),
        ("ville_presente",   "completude", F.col("ville").isNotNull()),
        ("temp_min_typable", "validite",   F.col("temp_min").isNotNull()),
        ("temp_max_typable", "validite",   F.col("temp_max").isNotNull()),
        ("temp_coherente",   "coherence",  F.col("temp_max") >= F.col("temp_min")),
        ("precip_positive",  "validite",   F.col("precip_mm") >= 0),
    ]
    dq_report("meteo", m, rules)
    ok, ko = apply_rules(m, rules)
    nq = to_quarantine("meteo", ko)

    (DeltaTable.forName(spark, "curated.meteo").alias("t")
     .merge(ok.drop("_brut").withColumn("_updated_at", F.current_timestamp()).alias("s"),
            "t.date = s.date AND t.ville = s.ville")
     .withSchemaEvolution()                   # R5 : vent_kmh ajouté sans casse
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    reconcile("meteo", lus, lus - apres, nq, ok.count())

# COMMAND ----------

# ---------------- API DISPONIBILITÉ
p = raw_path("api/disponibilite", f"ingest_date={RUN_DATE}")
if not path_exists(p):
    print("Pas de données API pour ce run.")
    dbutils.notebook.exit("no_api_data")

# Chaque ligne du JSON brut = 1 page de l'API ({total_count, results:[...]})
pages = spark.read.json(p)
recs = pages.select(F.explode_outer("results").alias("r")).filter(F.col("r").isNotNull()).select("r.*")
lus = recs.count()

d = recs.select(
    F.col("stationcode").cast("string").alias("stationcode"),
    F.col("name").alias("nom"),
    F.col("numbikesavailable").cast("int").alias("velos_dispo"),
    F.col("numdocksavailable").cast("int").alias("bornes_dispo"),
    F.col("capacity").cast("int").alias("capacite"),
    F.to_timestamp("duedate").alias("mesure_ts"),          # EVENT TIME fourni par l'API
)
d = latest_per_key(d, ["stationcode", "mesure_ts"], "mesure_ts")
apres = d.count()

# Données tardives : acceptées si la mesure date de moins de 7 jours (même fenêtre que R2), sinon quarantaine
limite = F.date_sub(F.lit(RUN_DATE).cast("date"), CORRECTION_DAYS)
rules = [
    ("stationcode_present", "completude", F.col("stationcode").isNotNull()),
    ("horodatage_valide",   "validite",   F.col("mesure_ts").isNotNull()),
    ("velos_non_negatif",   "validite",   F.col("velos_dispo") >= 0),
    ("pas_trop_tardive",    "fraicheur",  F.to_date("mesure_ts") >= limite),
]
dq_report("disponibilite", d, rules, seuils={"pas_trop_tardive": 0.90})
ok, ko = apply_rules(d, rules)
nq = to_quarantine("disponibilite", ko)

(DeltaTable.forName(spark, "curated.disponibilite").alias("t")
 .merge(ok.withColumn("event_date", F.to_date("mesure_ts"))
          .withColumn("_ingest_date", F.lit(RUN_DATE).cast("date"))
          .withColumn("_updated_at", F.current_timestamp()).alias("s"),
        "t.stationcode = s.stationcode AND t.mesure_ts = s.mesure_ts")
 .whenNotMatchedInsertAll()          # une mesure horodatée ne change pas : insert-only idempotent
 .execute())
reconcile("disponibilite", lus, lus - apres, nq, ok.count())
