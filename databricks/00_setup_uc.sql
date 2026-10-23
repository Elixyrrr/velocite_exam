-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Setup Unity Catalog (à exécuter UNE fois, à la main, en SQL warehouse ou cluster UC)
-- MAGIC Prérequis : storage credential `sc_velocite` créé depuis l'Access Connector
-- MAGIC (Catalog Explorer > External data > Credentials > Create, collez l'ID de ressource `AC_ID` de infra/outputs.env).
-- MAGIC
-- MAGIC Remplacez `ilyestristanflo2` par votre compte de stockage.

-- COMMAND ----------

-- Emplacements externes : un par conteneur (droits UC = 2e couche de contrôle au-dessus du RBAC Azure)
-- el_raw : le connecteur n'a que Storage Blob Data READER sur raw (immuable).
-- Si cette commande échoue sur la validation d'écriture, créez-la dans l'UI :
-- Catalog > External locations > Create > Advanced options > cocher "Read only".
CREATE EXTERNAL LOCATION IF NOT EXISTS el_raw
  URL 'abfss://raw@ilyestristanflo2.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL sc_velocite)
  COMMENT 'Zone raw — données brutes immuables, contient des données personnelles';
CREATE EXTERNAL LOCATION IF NOT EXISTS el_curated
  URL 'abfss://curated@ilyestristanflo2.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL sc_velocite);
CREATE EXTERNAL LOCATION IF NOT EXISTS el_consumption
  URL 'abfss://consumption@ilyestristanflo2.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL sc_velocite);
CREATE EXTERNAL LOCATION IF NOT EXISTS el_quarantine
  URL 'abfss://quarantine@ilyestristanflo2.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL sc_velocite);

-- COMMAND ----------

-- Catalogue : on utilise celui créé automatiquement avec le workspace (dbw_velocite)
USE CATALOG dbw_velocite;

CREATE SCHEMA IF NOT EXISTS curated
  MANAGED LOCATION 'abfss://curated@ilyestristanflo2.dfs.core.windows.net/tables'
  COMMENT 'Données nettoyées, typées, pseudonymisées. Propriétaire : équipe data.';
CREATE SCHEMA IF NOT EXISTS consumption
  MANAGED LOCATION 'abfss://consumption@ilyestristanflo2.dfs.core.windows.net/dashboard'
  COMMENT 'Agrégats du tableau de bord. Lecture seule pour les analystes.';
CREATE SCHEMA IF NOT EXISTS quarantine
  MANAGED LOCATION 'abfss://quarantine@ilyestristanflo2.dfs.core.windows.net/tables'
  COMMENT 'Lignes rejetées par les contrôles qualité, avec motif.';
CREATE SCHEMA IF NOT EXISTS ops
  MANAGED LOCATION 'abfss://quarantine@ilyestristanflo2.dfs.core.windows.net/ops'
  COMMENT 'Résultats DQ, réconciliation, journal RGPD.';

-- COMMAND ----------

-- ===== Tables curated : schéma EXPLICITE (R5 : les ajouts passent via mergeSchema, jamais de casse)
-- Partition par MOIS d'événement, pas par jour : ~1 500 locations/jour => partitions journalières = petits fichiers (A3).
CREATE TABLE IF NOT EXISTS curated.locations (
  location_id      INT        NOT NULL,
  abonne_pseudo    STRING     NOT NULL COMMENT 'SHA-256(abonne_id + sel Key Vault)',
  station_depart   INT,
  station_arrivee  INT,
  debut            TIMESTAMP  NOT NULL,
  fin              TIMESTAMP  NOT NULL,
  duree_min        DOUBLE,
  montant          DECIMAL(6,2),
  event_date       DATE       NOT NULL COMMENT 'Date de DÉBUT de location (A2)',
  event_month      STRING     NOT NULL,
  modifie_le       TIMESTAMP  NOT NULL,
  _ingest_date     DATE,
  _updated_at      TIMESTAMP
) USING DELTA
PARTITIONED BY (event_month)
COMMENT 'Locations pseudonymisées. Grain : 1 ligne par location_id.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

-- DROP IF EXISTS avant ADD : le notebook reste relançable sans erreur
ALTER TABLE curated.locations DROP CONSTRAINT IF EXISTS chk_duree;
ALTER TABLE curated.locations ADD CONSTRAINT chk_duree CHECK (fin >= debut);
ALTER TABLE curated.locations DROP CONSTRAINT IF EXISTS chk_montant;
ALTER TABLE curated.locations ADD CONSTRAINT chk_montant CHECK (montant > 0);

CREATE TABLE IF NOT EXISTS curated.abonnes (
  abonne_pseudo  STRING NOT NULL,
  ville          STRING,
  modifie_le     TIMESTAMP NOT NULL,
  _updated_at    TIMESTAMP
) USING DELTA COMMENT 'Abonnés SANS nom ni e-mail (minimisation RGPD).';

CREATE TABLE IF NOT EXISTS curated.stations (
  station_id  INT NOT NULL,
  nom         STRING,
  capacite    INT,
  _updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS curated.meteo (
  date        DATE NOT NULL,
  ville       STRING NOT NULL,
  temp_min    DOUBLE,
  temp_max    DOUBLE,
  precip_mm   DOUBLE,
  _source_file STRING,
  _updated_at TIMESTAMP
) USING DELTA COMMENT 'Météo quotidienne ; colonnes additionnelles ajoutées automatiquement (R5).';

CREATE TABLE IF NOT EXISTS curated.disponibilite (
  stationcode        STRING NOT NULL,
  nom                STRING,
  velos_dispo        INT,
  bornes_dispo       INT,
  capacite           INT,
  mesure_ts          TIMESTAMP NOT NULL COMMENT 'Horodatage de la mesure (event time)',
  event_date         DATE,
  _ingest_date       DATE,
  _updated_at        TIMESTAMP
) USING DELTA PARTITIONED BY (event_date);

-- COMMAND ----------

-- ===== Ops
CREATE TABLE IF NOT EXISTS ops.dq_results (
  run_date DATE, table_name STRING, dimension STRING, regle STRING,
  total BIGINT, ko BIGINT, taux_ok DOUBLE, seuil DOUBLE, statut STRING, checked_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ops.reconciliation (
  run_date DATE, table_name STRING, lus_raw BIGINT, doublons BIGINT,
  quarantaine BIGINT, merges BIGINT, ecart BIGINT, statut STRING, checked_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ops.rgpd_log (
  abonne_pseudo STRING, demande_le TIMESTAMP, lignes_supprimees BIGINT, tables STRING, operateur STRING
) USING DELTA COMMENT 'Traçabilité des effacements — aucune donnée directement identifiante.';

-- COMMAND ----------

-- ===== Droits (moindre privilège côté UC) — créez ces groupes dans le workspace / Entra
GRANT USE CATALOG ON CATALOG dbw_velocite TO `grp-velocite-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA dbw_velocite.consumption TO `grp-velocite-analysts`;
-- Aucun droit des analystes sur curated / quarantine / raw.
GRANT USE CATALOG ON CATALOG dbw_velocite TO `grp-velocite-dataeng`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dbw_velocite.curated TO `grp-velocite-dataeng`;
GRANT READ FILES ON EXTERNAL LOCATION el_raw TO `grp-velocite-dataeng`;

-- Preuve : SHOW GRANTS ON SCHEMA dbw_velocite.consumption;
