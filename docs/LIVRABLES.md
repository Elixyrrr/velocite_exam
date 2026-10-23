# VéloCité – Livrables Partie 3

Oct 23, 2026 · Groupe VéloCité

Le pipeline tourne de bout en bout sur Azure : Azure SQL → ADF (watermark) → raw → Databricks → curated → consumption, sans aucun secret en clair.

## Bilan des critères

Tous les critères minimum du sujet sont couverts ; deux écarts sont assumés (alerte portée par Databricks, sources météo et API non déployées).

| Brique | Implémentation | Preuve |
| --- | --- | --- |
| Source | Azure SQL serverless (auto-pause 1 h), script du sujet chargé : 200 000 locations, 5 000 abonnés, 50 stations | `SELECT COUNT(*)` |
| Lac | ADLS Gen2, 5 conteneurs : landing, raw, curated, consumption, quarantine | arborescence `raw/sql/<table>/ingest_date=…/<runId>.parquet` |
| Ingestion | ADF `P01_RAW` : Lookup watermark → Copy (fenêtre watermark − 7 j) → mise à jour du watermark → journal `etl.run_log`, retries 3 × 60 s | run 1 : 200 000 lignes ; run 2 : 8 846 lignes |
| Transformation | Job Databricks `velocite_daily` (serverless) : typage, dédoublonnage, MERGE, agrégats sur date d'événement | run vert en 1 min 51 |
| Qualité | 5 dimensions mesurées dans `ops.dq_results` (complétude, validité, unicité, cohérence, fraîcheur), rejets dans `quarantine.*` avec motif, réconciliation dans `ops.reconciliation` | écart = 0 |
| Sécurité | Identités managées (ADF, Access Connector), clé de stockage désactivée, SQL Entra-only, sel dans Key Vault, RBAC par conteneur | 403 Databricks en écriture sur raw |
| Gouvernance | Unity Catalog : catalogue commenté, propriétaires, lineage (Purview non utilisé) | graphe de lineage `activite_reseau_jour` |
| Observabilité et coûts | Notification d'échec du job Databricks testée, budget 50 €/mois à 50/80/100 % | mail d'échec reçu |

Écarts assumés :

- L'action group Azure Monitor est refusé par la politique de régions de l'abonnement Azure for Students : la notification d'échec passe par le job Databricks.
- Les sources météo (CSV) et API (R4) sont conçues et codées (notebook `03_curated_meteo_api`, pipeline `pl_ingest_meteo`) mais non déployées faute de temps.
- L'orchestration ADF → Databricks n'est pas encore branchée : cible = activité Job Databricks en fin de `P01_RAW`.

## ADR

### ADR-01 – Transformation sur Databricks (job serverless) plutôt qu'ADF Data Flow

- **Contexte** : il faut typer, dédoublonner, pseudonymiser, mettre en quarantaine et faire un MERGE idempotent vers des tables Delta (A4, A6, R2).
- **Décision** : notebooks PySpark exécutés par un job Databricks `velocite_daily` en compute serverless, Unity Catalog pour les tables.
- **Alternatives** : ADF Mapping Data Flow (pas de MERGE Delta simple, quarantaine et sel Key Vault plus lourds) ; Synapse Spark (moins outillé pour Delta et le lineage).
- **Conséquences** : MERGE, OPTIMIZE et évolution de schéma natifs ; lineage automatique ; pas de cluster à gérer ni à oublier allumé. En contrepartie, un deuxième outil à maîtriser et un coût à la DBU.

### ADR-02 – Un conteneur par zone, droits RBAC par conteneur

- **Contexte** : A7 (tout le monde Contributor sur le stockage) et R3 (raw contient des données personnelles).
- **Décision** : conteneurs `landing`, `raw`, `curated`, `consumption`, `quarantine`. ADF : Contributor sur landing et raw uniquement. Access Connector Databricks : Reader sur raw, Contributor sur les trois autres, Storage Blob Delegator sur le compte. Clé de compte désactivée.
- **Alternatives** : un conteneur unique avec des dossiers et des ACL POSIX (plus fin mais difficile à auditer) ; un compte de stockage par zone (isolation forte mais coût et gestion multipliés).
- **Conséquences** : raw est immuable pour Databricks (403 prouvé à l'écriture) ; un clic IAM par périmètre ; le rôle Delegator est nécessaire pour Unity Catalog mais ne donne aucun accès aux données.

### ADR-03 – Watermark avec fenêtre de 7 jours + MERGE idempotent

- **Contexte** : R1 (dashboard J-1 avant 6 h 30), R2 (corrections jusqu'à 7 jours), A1 (relecture complète), A6 (doublons au rejeu).
- **Décision** : ADF lit `modifie_le > watermark − 7 jours` et `≤ heure du run`, stocke le watermark dans `etl.control` et l'avance seulement après une copie réussie. Databricks garde la dernière version par clé et fait un MERGE `WHEN MATCHED AND s.modifie_le > t.modifie_le`. Consumption recalcule J-8 à J-1 en `replaceWhere`.
- **Alternatives** : watermark strict sans fenêtre (rate une correction si l'horloge ou une transaction longue décale `modifie_le`) ; Change Data Capture SQL (plus précis mais plus lourd à opérer) ; rechargement complet quotidien (200 000 lignes à chaque run).
- **Conséquences** : 8 846 lignes lues au lieu de 200 000 au 2e run (−96 %) ; un rejeu ne crée aucun doublon ; la table `stations`, sans colonne `modifie_le`, reste en copie complète (50 lignes).

### ADR-04 – Pseudonymisation SHA-256 salée et minimisation

- **Contexte** : R3 (RGPD) : les analystes n'ont pas besoin de l'identité des abonnés, le DPO exige l'effacement sur demande.
- **Décision** : `abonne_pseudo = SHA-256(abonne_id || sel)`, sel stocké dans Key Vault (`pseudo-salt`) et lu via un secret scope ; nom et e-mail ne sortent jamais de raw ; raw supprimé à 30 jours (lifecycle) ; effacement ciblé par recalcul du pseudo, `DELETE`, `REORG … APPLY (PURGE)` puis `VACUUM`.
- **Alternatives** : hachage sans sel (réversible par force brute sur 5 000 identifiants) ; table de correspondance id ↔ pseudo (une donnée sensible de plus à protéger) ; chiffrement réversible (inutile pour l'analyse).
- **Conséquences** : jointures toujours possibles ; `abonne_pseudo` reste une donnée personnelle (pseudonyme, pas anonyme) ; changer le sel impose de re-pseudonymiser curated.

### ADR-05 – Calcul serverless et ressources à la demande pour maîtriser les coûts

- **Contexte** : R6 (budget maîtrisé) ; un seul traitement par nuit, ~200 000 lignes.
- **Décision** : Azure SQL serverless avec auto-pause 1 h ; job Databricks serverless en mode standard (performance optimized désactivé) ; stockage LRS ; budget Cost Management de 50 € par mois.
- **Alternatives** : SQL provisionné (facturé 24 h/24) ; cluster Databricks all-purpose (risque d'oubli allumé) ; stockage GRS (environ 2 fois plus cher, inutile ici).
- **Conséquences** : coût quasi nul hors exécution ; démarrage à froid de quelques minutes, absorbé par la marge entre le déclenchement à 4 h et l'échéance de 6 h 30.

## Runbook

Astreinte : mail d'échec du job Databricks `velocite_daily`. Échéance métier : données J-1 dans le dashboard avant 6 h 30.

### 1. Relancer un run en échec

| Symptôme | Action |
| --- | --- |
| Copy SQL en échec (timeout, base en pause) | ADF → Monitor → Rerun de `P01_RAW` ; la base serverless se réveille au premier appel, les 3 retries couvrent souvent le cas |
| Procedure stockée en échec | vérifier `etl.control` (3 lignes) et les droits `EXECUTE` de l'identité ADF sur le schéma `etl` |
| Job Databricks en échec | Jobs & Pipelines → `velocite_daily` → run rouge → lire l'erreur de la tâche → corriger → Repair run |
| Réconciliation KO | comparer `etl.run_log` (SQL) et `ops.reconciliation` (Databricks) pour la même date |

Relancer est sans risque : chaque copie écrit un nouveau fichier `<runId>.parquet`, le watermark n'avance qu'après succès, le MERGE absorbe les doublons.

### 2. Rejouer une fenêtre

1. Derniers 7 jours : rien à faire, chaque run relit la fenêtre et recalcule J-8 à J-1.
2. Fenêtre plus ancienne : `UPDATE etl.control SET last_value = '<date de début>' WHERE table_name = 'locations';`
3. Lancer `P01_RAW` (Trigger now), puis le job `velocite_daily` avec `run_date` = date du jour et `full_refresh = true`.
4. Contrôler : `SELECT * FROM dbw_velocite.ops.reconciliation ORDER BY checked_at DESC` → statut OK, écart 0.

### 3. Traiter la quarantaine

```sql
SELECT motif, count(*) FROM dbw_velocite.quarantine.locations
WHERE _statut = 'A_TRAITER' GROUP BY motif;
```

| Cause | Traitement |
| --- | --- |
| Erreur de saisie source (fin < début, montant ≤ 0) | correction dans Azure SQL par le métier : `modifie_le` avance, la ligne revient au run suivant |
| Référentiel incomplet (station inconnue) | ajouter la station en source puis relancer |
| Règle trop stricte | modifier la règle, rejouer la fenêtre (§2) |

Clôture : `UPDATE dbw_velocite.quarantine.locations SET _statut = 'TRAITE' WHERE …;` Seuil d'alerte : plus de 1 % des lignes du jour (statut ALERTE dans `ops.dq_results`).

### 4. Effacement RGPD d'un abonné (délai légal : 1 mois)

1. Métier : `DELETE` de l'abonné dans `dbo.locations` et `dbo.abonnes`.
2. Databricks : notebook `05_rgpd_erase` avec `abonne_id` → suppression en curated et quarantaine, purge physique, trace dans `ops.rgpd_log`.
3. raw : suppression automatique à 30 jours (lifecycle).
4. consumption : agrégats anonymes, recalculés au run suivant.

## Estimation des coûts

Le run quotidien coûte environ 30 $ par mois (≈ 28 €), sous le budget de 50 € ; Databricks et Azure SQL en font l'essentiel. Prix unitaires approximatifs (tarifs publics de référence en $, région US) : Switzerland North est en général 20 à 25 % plus cher, chaque ligne est à ressaisir dans le [Azure Pricing Calculator](https://azure.microsoft.com/pricing/calculator/).

**Hypothèses** : 1 run par jour (30 par mois) ; ~9 000 lignes copiées par run en régime incrémental ; 3 copies ADF, 2 lookups, 4 procédures stockées ; job Databricks de ~2 min ; volume total du lac < 1 Go ; base SQL ~1 h active par jour (délai d'auto-pause compris).

| Service | Ligne du calculateur | Calcul | Coût mensuel ($) |
| --- | --- | --- | --- |
| Azure Databricks | Serverless jobs compute, Premium | ~1,5 DBU/run × 30 runs × ~0,35 $/DBU | ~16 |
| Azure SQL Database | General Purpose serverless, Gen5, 0,5 à 1 vCore, auto-pause 60 min | 0,5 vCore × ~4 000 s/jour × 30 × 0,000145 $ + stockage | ~10 |
| Data Factory | Azure IR : orchestration + mouvement de données | 300 exécutions × 1 $/1 000 + 3 copies × 4 DIU × 1 min × 30 × 0,25 $/DIU-h | ~2 |
| ADLS Gen2 | Hot, LRS, < 1 Go + transactions | 0,02 $/Go + quelques milliers d'opérations | < 1 |
| Key Vault | Standard, opérations secrets | ~30 lectures du sel par mois | < 0,01 |
| **Total** |  |  | **~30 (≈ 28 €)** |

Leviers : l'offre gratuite Azure SQL (100 000 vCore-secondes par mois) ramène la ligne SQL près de 0 ; le mode standard du job serverless (performance optimized désactivé) réduit le prix de la DBU ; un passage à 10 fois plus de données ferait surtout croître la ligne Databricks.

## Mises à jour du schéma

Le schéma `archi.xml` reste valable ; cinq annotations sont à aligner sur ce qui est déployé.

| Boîte | Avant | Après |
| --- | --- | --- |
| CURATED | Partition : date d'événement | Partition : mois d'événement (`event_month`) — évite les petits fichiers (A3) ; calculs toujours sur `event_date` (A2) |
| Lac | raw, curated, consumption, quarantaine | ajouter `landing` (dépôt CSV météo) ; noter « 1 conteneur = 1 périmètre RBAC » |
| TRANSFORMATION | Transformation | Azure Databricks — job serverless `velocite_daily`, Unity Catalog |
| GOUVERNANCE | Microsoft Purview | Unity Catalog (catalogue, propriétaires, lineage) ; Purview cible en production |
| OBSERVABILITÉ | Azure Monitor · alertes | Notification d'échec Databricks + journal `etl.run_log` + `ops.reconciliation` + budget ; action group Azure Monitor cible en production |

Ajouter aussi une flèche « watermark (etl.control) » entre Azure SQL et ADF, et la flèche pointillée cible « ADF déclenche le job Databricks ».

## Preuves à capturer

Le sujet demande quatre familles de captures : alerte, droits, lineage, contrôles qualité.

**Alerte**

- [ ] Run Databricks en échec avec son message d'erreur
- [ ] Mail de notification reçu
- [ ] Erreur de l'action group Azure Monitor (politique de régions)
- [ ] Page du budget Cost Management

**Droits et secrets**

- [ ] IAM du conteneur `raw` (ADF Contributor, ac-velocite Reader)
- [ ] IAM du conteneur `curated` (ac-velocite Contributor, pas d'ADF)
- [ ] Erreur 403 de Databricks en écriture sur raw
- [ ] Stockage : accès par clé désactivé ; SQL : Entra-only coché
- [ ] Linked service SQL en identité managée
- [ ] Key Vault : secret `pseudo-salt`, valeur masquée

**Lineage**

- [ ] Graphe et détail du lineage de `consumption.activite_reseau_jour` (notebook `04_consumption`, job `velocite_daily`)

**Qualité et ingestion**

- [ ] `ops.dq_results` (5 dimensions)
- [ ] `ops.reconciliation` (écart = 0)
- [ ] `quarantine.locations` avec la colonne `motif`
- [ ] `etl.run_log` : 200 000 puis 8 846 lignes
- [ ] `curated.abonnes` sans nom ni e-mail

**Export du pipeline**

- [ ] ADF Studio → Manage → ARM template → Export ARM template (zip à joindre au rendu)

