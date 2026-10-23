-- =============================================================================
-- VéloCité — scénarios de démo pour l'oral (à lancer APRÈS un premier run complet)
-- Chaque bloc illustre une anomalie d'audit corrigée.
-- =============================================================================

-- [R2 / A6] Correction rétroactive J-3 : montant corrigé, modifie_le avancé.
-- Attendu : run suivant => 1 ligne MISE À JOUR en curated, 0 doublon. Relancer 2x => même résultat.
UPDATE dbo.locations
SET montant = montant + 2.50, modifie_le = SYSUTCDATETIME()
WHERE location_id IN (4000, 4001, 4002);

-- [A4] Lignes invalides : attendu => en quarantaine avec motif, PAS perdues silencieusement.
INSERT INTO dbo.locations VALUES
 (900001, 12, 3, 8, SYSUTCDATETIME(), DATEADD(MINUTE,-30,SYSUTCDATETIME()), 4.00, SYSUTCDATETIME()), -- fin < debut
 (900002, 13, 3, 8, DATEADD(MINUTE,-20,SYSUTCDATETIME()), NULL, 3.00, SYSUTCDATETIME()),            -- fin manquante
 (900003, 14, 99, 8, DATEADD(MINUTE,-20,SYSUTCDATETIME()), SYSUTCDATETIME(), -1.00, SYSUTCDATETIME()); -- station inconnue + montant négatif

-- [A2] Location à cheval sur minuit : début hier 23:55, fin aujourd'hui 00:10.
-- Attendu : comptée sur la date de DÉBUT (date d'événement), pas sur la date d'ingestion.
INSERT INTO dbo.locations VALUES
 (900010, 20, 5, 6,
  DATEADD(MINUTE, -5, CAST(CAST(SYSUTCDATETIME() AS DATE) AS DATETIME2)),
  DATEADD(MINUTE, 10, CAST(CAST(SYSUTCDATETIME() AS DATE) AS DATETIME2)),
  3.00, SYSUTCDATETIME());

-- [R5] Évolution de schéma côté source : nouvelle colonne nullable.
-- Attendu : le pipeline ne casse pas ; la colonne apparaît en curated (mergeSchema).
ALTER TABLE dbo.locations ADD type_velo NVARCHAR(20) NULL;
UPDATE dbo.locations SET type_velo = 'electrique', modifie_le = SYSUTCDATETIME() WHERE location_id BETWEEN 100 AND 110;

-- [R3] Demande d'effacement RGPD de l'abonné 42 : voir notebook 05_rgpd_erase + runbook.
-- DELETE FROM dbo.locations WHERE abonne_id = 42;
-- DELETE FROM dbo.abonnes   WHERE abonne_id = 42;
