# VéloCité – Lakehouse Azure (Partie 3)

Pipeline quotidien : **Azure SQL → ADF (watermark) → ADLS raw → Databricks → curated → consumption**, sans aucun secret en clair.

Documents du rendu (ADR, runbook, coûts, preuves) : [`docs/LIVRABLES.md`](docs/LIVRABLES.md)

## Contenu du dépôt

```
adf/          export ARM d'Azure Data Factory (pipeline P01_RAW, datasets, linked services)
databricks/   notebooks du job velocite_daily + export JSON du job
sql/          script source du sujet, schéma de contrôle ETL (watermark, journal), scénarios de démo
docs/         livrables écrits (ADR, runbook, estimation des coûts, schéma)
preuves/      captures (alerte, droits, lineage, qualité)
```

## Arborescence du lac (ADLS Gen2 `ilyestristanflo2`)

| Conteneur | Écrit par | Contenu |
| --- | --- | --- |
| `landing` | dépôt manuel | `meteo/` : CSV météo avant ingestion |
| `raw` | ADF (immuable, lecture seule pour Databricks) | `sql/<table>/ingest_date=YYYY-MM-DD/<RunId>.parquet` ; supprimé à 30 j pour abonnes et locations |
| `curated` | Databricks | tables Delta `dbw_velocite.curated.*` : abonnes (pseudonymisés), locations, stations, meteo, disponibilite |
| `consumption` | Databricks | `activite_station_jour`, `activite_reseau_jour` (agrégats par date d'événement) |
| `quarantine` | Databricks | `quarantine.*` (lignes rejetées + motif) et `ops.*` (dq_results, reconciliation, rgpd_log) |

## Ordre d'exécution

1. `sql/01_schema_seed.sql` puis `sql/02_etl_control.sql` (Query editor, connexion Entra)
2. `databricks/00_setup_uc.sql` (une fois : external locations, schémas, tables)
3. ADF `P01_RAW` → raw
4. Job Databricks `velocite_daily` : `01_curated_referentiels` → `02_curated_locations` / `03_curated_meteo_api` → `04_consumption`
5. Démo des anomalies : `sql/03_demo_anomalies.sql`, puis relancer 3 et 4
