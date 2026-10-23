# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — curated → consumption : agrégats du tableau de bord
# MAGIC - Agrégation sur la **date d'événement** (date de début) — A2
# MAGIC - Recalcul de la fenêtre glissante J-8 → J-1 à chaque run : les corrections rétroactives (R2) se propagent
# MAGIC - Écriture `replaceWhere` : idempotente, relançable (A6)
# MAGIC - Aucune donnée personnelle : uniquement des comptages (R3)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

run_d = dt.date.fromisoformat(RUN_DATE)
d_max = run_d - dt.timedelta(days=1)                       # J-1 : le jour attendu à 6h30 (R1)
d_min = run_d - dt.timedelta(days=CORRECTION_DAYS + 1)     # fenêtre de corrections

loc = spark.table("curated.locations")
if FULL_REFRESH:
    bounds = loc.agg(F.min("event_date"), F.max("event_date")).first()
    if bounds[0] is None:
        dbutils.notebook.exit("curated.locations vide : rien à agréger")
    d_min, d_max = bounds[0], bounds[1]
print(f"Fenêtre recalculée : {d_min} → {d_max}")

loc = loc.filter(F.col("event_date").between(F.lit(d_min), F.lit(d_max)))
stations = spark.table("curated.stations").select("station_id", F.col("nom").alias("station_nom"), "capacite")
meteo_paris = (spark.table("curated.meteo").filter(F.col("ville") == "Paris")   # hypothèse : réseau parisien
               .select(F.col("date").alias("event_date"), "temp_max", "precip_mm"))

# COMMAND ----------

# --- Agrégat 1 : activité quotidienne par station de départ (grain dashboard)
par_station = (loc.groupBy("event_date", F.col("station_depart").alias("station_id"))
               .agg(F.count("*").alias("nb_locations"),
                    F.countDistinct("abonne_pseudo").alias("nb_abonnes_actifs"),
                    F.sum("montant").alias("chiffre_affaires"),
                    F.round(F.avg("duree_min"), 1).alias("duree_moy_min"))
               .join(stations, "station_id", "left")
               .withColumn("taux_rotation", F.round(F.col("nb_locations") / F.col("capacite"), 2))
               .withColumn("_computed_at", F.current_timestamp()))

# --- Agrégat 2 : synthèse réseau quotidienne + météo (feature store du futur modèle ML)
reseau = (loc.groupBy("event_date")
          .agg(F.count("*").alias("nb_locations"),
               F.countDistinct("abonne_pseudo").alias("nb_abonnes_actifs"),
               F.sum("montant").alias("chiffre_affaires"),
               F.round(F.avg("duree_min"), 1).alias("duree_moy_min"))
          .join(meteo_paris, "event_date", "left")
          .withColumn("jour_semaine", F.date_format("event_date", "E"))
          .withColumn("_computed_at", F.current_timestamp()))

where = f"event_date >= '{d_min}' AND event_date <= '{d_max}'"
for name, df in [("activite_station_jour", par_station), ("activite_reseau_jour", reseau)]:
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", where)
       .option("mergeSchema", "true")
       .saveAsTable(f"consumption.{name}"))
    print(f"consumption.{name} : {df.count()} lignes réécrites sur la fenêtre")

# COMMAND ----------

# --- Contrôle R1 : J-1 doit être présent, sinon le job échoue => alerte ADF avant 6h30
n_j1 = spark.table("consumption.activite_reseau_jour").filter(F.col("event_date") == F.lit(run_d - dt.timedelta(days=1))).count()
if n_j1 == 0 and not FULL_REFRESH:
    raise Exception(f"R1 KO : aucune activité pour J-1 ({run_d - dt.timedelta(days=1)}) dans consumption")

# --- Réconciliation des comptages curated vs consumption (observabilité)
src = loc.count()
dst = (spark.table("consumption.activite_station_jour")
       .filter(F.col("event_date").between(F.lit(d_min), F.lit(d_max)))
       .agg(F.sum("nb_locations")).first()[0] or 0)
reconcile("consumption.activite_station_jour", src, 0, 0, dst)
