# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Effacement RGPD d'un abonné, de bout en bout (R3, droit à l'effacement art. 17)
# MAGIC Lancé à la demande (pas dans le job quotidien). Ordre complet : voir RUNBOOK.md §4.
# MAGIC
# MAGIC | Zone | Mécanisme |
# MAGIC |---|---|
# MAGIC | Source SQL | DELETE par le métier (sinon la donnée revient) |
# MAGIC | raw | Pas de modif (immuable) : **rétention 30 j** par lifecycle policy, accès restreint |
# MAGIC | curated / quarantine | DELETE sur `abonne_pseudo` recalculé, REORG PURGE (deletion vectors) puis VACUUM (purge physique) |
# MAGIC | consumption | Agrégats anonymes : recalcul de la fenêtre pour cohérence |
# MAGIC | ops.rgpd_log | Trace de la demande, sans donnée identifiante |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("abonne_id", "")
abonne_id = dbutils.widgets.get("abonne_id").strip()
if not abonne_id.isdigit():
    raise ValueError("Paramètre abonne_id obligatoire (entier)")

# Le pseudo est recalculé avec le sel : pas besoin de table de correspondance id <-> pseudo
ps = spark.range(1).select(pseudo(F.lit(int(abonne_id))).alias("p")).first()["p"]

total = 0
tables = ["curated.locations", "curated.abonnes", "quarantine.locations", "quarantine.abonnes"]
dates = [r[0] for r in spark.table("curated.locations").filter(F.col("abonne_pseudo") == ps)
                                .select("event_date").distinct().collect()]
for t in tables:
    if not spark.catalog.tableExists(t):
        continue
    dt_ = DeltaTable.forName(spark, t)
    dt_.delete(F.col("abonne_pseudo") == ps)
    n = int(dt_.history(1).select("operationMetrics").first()[0].get("numDeletedRows", 0) or 0)
    total += n
    print(f"{t}: {n} ligne(s) supprimée(s)")

spark.createDataFrame(
    [(ps, total, ",".join(tables), spark.sql("SELECT current_user()").first()[0])],
    "abonne_pseudo string, lignes_supprimees bigint, tables string, operateur string",
).withColumn("demande_le", F.current_timestamp()) \
 .select("abonne_pseudo", "demande_le", "lignes_supprimees", "tables", "operateur") \
 .write.mode("append").saveAsTable("ops.rgpd_log")

print(f"Dates d'activité impactées (à recalculer en consumption) : {sorted(dates)}")

# COMMAND ----------

# Purge physique des anciennes versions Delta (sinon time travel = donnée toujours lisible).
# 168 h = rétention par défaut : l'effacement physique est effectif sous 7 jours (délai RGPD : 1 mois).
for t in tables:
    if spark.catalog.tableExists(t):
        # Deletion vectors (activés par défaut) : le DELETE ne fait que "marquer" les lignes.
        # REORG ... APPLY (PURGE) réécrit les fichiers sans elles ; VACUUM supprime ensuite les anciens.
        spark.sql(f"REORG TABLE {t} APPLY (PURGE)")
        spark.sql(f"VACUUM {t} RETAIN 168 HOURS")
