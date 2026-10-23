# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Configuration et fonctions communes
# MAGIC Inclus par chaque notebook via `%run ./00_config`. Aucun secret ici : le sel vient de Key Vault.

# COMMAND ----------

import datetime as dt
import uuid
from pyspark.sql import DataFrame, functions as F, Window
from delta.tables import DeltaTable

STORAGE = "ilyestristanflo2"           # <-- votre compte ADLS
CATALOG = "dbw_velocite"              # catalogue créé automatiquement avec le workspace
SECRET_SCOPE = "kv-velocite"          # secret scope adossé à Key Vault
SALT_KEY = "pseudo-salt"
CORRECTION_DAYS = 7                   # R2

RAW = f"abfss://raw@{STORAGE}.dfs.core.windows.net"

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("full_refresh", "false")
RUN_DATE = dbutils.widgets.get("run_date") or dt.date.today().isoformat()
FULL_REFRESH = dbutils.widgets.get("full_refresh").lower() == "true"
RUN_ID = str(uuid.uuid4())

spark.sql(f"USE CATALOG {CATALOG}")
# R5 : l'évolution de schéma des MERGE passe par .withSchemaEvolution() (compatible serverless,
# qui interdit spark.databricks.delta.schema.autoMerge.enabled)
spark.conf.set("spark.sql.session.timeZone", "UTC")

print(f"run_date={RUN_DATE} full_refresh={FULL_REFRESH} run_id={RUN_ID}")

# COMMAND ----------

def pseudo(col):
    """Pseudonymisation déterministe : SHA-256(id || sel secret).
    Déterministe => jointures possibles entre tables et effacement ciblé (on recalcule le pseudo).
    Sans le sel (Key Vault), impossible de revenir à l'ID par force brute (5 000 IDs sinon triviaux)."""
    salt = dbutils.secrets.get(SECRET_SCOPE, SALT_KEY)
    return F.sha2(F.concat(col.cast("string"), F.lit(salt)), 256)


def raw_path(*parts: str) -> str:
    return "/".join([RAW, *parts])


def path_exists(p: str) -> bool:
    try:
        dbutils.fs.ls(p)
        return True
    except Exception:
        return False


def apply_rules(df: DataFrame, rules: list):
    """rules = [(nom_regle, dimension, condition_OK)] -> (df_ok, df_ko avec colonne motif).
    Une condition NULL est considérée KO (pas de perte silencieuse : A4)."""
    fails = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, _, cond in rules]
    flagged = df.withColumn("_motif", F.concat_ws(",", *fails))
    ok = flagged.filter(F.col("_motif") == "").drop("_motif")
    ko = flagged.filter(F.col("_motif") != "").withColumnRenamed("_motif", "motif")
    return ok, ko


def dq_report(table: str, df: DataFrame, rules: list, seuils: dict | None = None):
    """Mesure chaque règle (complétude / validité / unicité...) et écrit dans ops.dq_results."""
    seuils = seuils or {}
    total = df.count()
    aggs = [F.sum(F.when(~F.coalesce(cond, F.lit(False)), 1).otherwise(0)).alias(name)
            for name, _, cond in rules]
    kos = df.agg(*aggs).first().asDict() if total else {name: 0 for name, _, _ in rules}
    rows = []
    for name, dim, _ in rules:
        ko = int(kos[name] or 0)
        taux = 1.0 if total == 0 else 1 - ko / total
        seuil = seuils.get(name, 0.95)
        rows.append((dt.date.fromisoformat(RUN_DATE), table, dim, name, total, ko, taux, seuil,
                     "OK" if taux >= seuil else "ALERTE"))
    out = (spark.createDataFrame(rows, "run_date date, table_name string, dimension string, regle string, "
                                       "total bigint, ko bigint, taux_ok double, seuil double, statut string")
           .withColumn("checked_at", F.current_timestamp()))
    out.write.mode("append").saveAsTable("ops.dq_results")
    display(out)
    return out


def dq_uniqueness(table: str, total: int, distinct: int, seuil: float = 0.0):
    """Unicité mesurée avant dédoublonnage (les doublons sont attendus avec le lookback R2 : info, pas alerte)."""
    dup = total - distinct
    spark.createDataFrame(
        [(dt.date.fromisoformat(RUN_DATE), table, "unicite", "cle_unique", total, dup,
          1.0 if total == 0 else distinct / total, seuil, "INFO")],
        "run_date date, table_name string, dimension string, regle string, total bigint, ko bigint, "
        "taux_ok double, seuil double, statut string",
    ).withColumn("checked_at", F.current_timestamp()).write.mode("append").saveAsTable("ops.dq_results")


def to_quarantine(table: str, df_ko: DataFrame):
    n = df_ko.count()
    if n:
        (df_ko.withColumn("_run_date", F.lit(RUN_DATE).cast("date"))
              .withColumn("_run_id", F.lit(RUN_ID))
              .withColumn("_quarantined_at", F.current_timestamp())
              .withColumn("_statut", F.lit("A_TRAITER"))
              .write.mode("append").option("mergeSchema", "true")
              .saveAsTable(f"quarantine.{table}"))
    print(f"quarantaine {table}: {n} ligne(s)")
    return n


def reconcile(table: str, lus: int, doublons: int, quarantaine: int, merges: int):
    """Réconciliation des comptages : lus = doublons + quarantaine + mergés. Écart != 0 => échec du job."""
    ecart = lus - doublons - quarantaine - merges
    statut = "OK" if ecart == 0 else "ECART"
    spark.createDataFrame(
        [(dt.date.fromisoformat(RUN_DATE), table, lus, doublons, quarantaine, merges, ecart, statut)],
        "run_date date, table_name string, lus_raw bigint, doublons bigint, quarantaine bigint, "
        "merges bigint, ecart bigint, statut string",
    ).withColumn("checked_at", F.current_timestamp()).write.mode("append").saveAsTable("ops.reconciliation")
    print(f"réconciliation {table}: lus={lus} doublons={doublons} quarantaine={quarantaine} mergés={merges} écart={ecart}")
    if ecart != 0:
        raise Exception(f"Réconciliation KO sur {table} : écart {ecart}")


def latest_per_key(df: DataFrame, keys: list, order_col: str) -> DataFrame:
    """Garde la version la plus récente par clé.
    Les lignes à clé NULL ne sont PAS dédoublonnées (sinon elles seraient fusionnées en une seule
    et comptées comme doublons) : elles passent telles quelles et finissent en quarantaine (A4)."""
    null_key = F.lit(False)
    for k in keys:
        null_key = null_key | F.col(k).isNull()
    sans_cle = df.filter(null_key)
    avec_cle = df.filter(~null_key)
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    dedup = avec_cle.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return dedup.unionByName(sans_cle)
