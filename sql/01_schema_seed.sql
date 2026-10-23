-- =============================================================================
-- VéloCité — source transactionnelle (script du sujet) + index watermark
-- À exécuter connecté en admin Entra (Query editor du portail ou Azure Data Studio)
-- =============================================================================

CREATE TABLE dbo.abonnes (abonne_id INT PRIMARY KEY,
  nom NVARCHAR(80), email NVARCHAR(120), ville NVARCHAR(40),
  modifie_le DATETIME2);
CREATE TABLE dbo.stations (station_id INT PRIMARY KEY,
  nom NVARCHAR(80), capacite INT);
CREATE TABLE dbo.locations (location_id INT PRIMARY KEY,
  abonne_id INT, station_depart INT, station_arrivee INT,
  debut DATETIME2, fin DATETIME2, montant DECIMAL(6,2),
  modifie_le DATETIME2);

INSERT INTO dbo.abonnes SELECT value, CONCAT('Nom', value),
  CONCAT('abonne', value, '@mail.test'),
  CHOOSE(value % 4 + 1, 'Paris', 'Lyon', 'Lille', 'Nantes'),
  DATEADD(MINUTE, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 5000);

INSERT INTO dbo.stations SELECT value,
  CONCAT('Station ', value), 10 + value % 30
FROM GENERATE_SERIES(1, 50);

INSERT INTO dbo.locations SELECT value, value % 5000 + 1,
  value % 50 + 1, (value * 7) % 50 + 1,
  DATEADD(MINUTE, -value, SYSUTCDATETIME()),
  DATEADD(MINUTE, 10 - value, SYSUTCDATETIME()), 1 + value % 9,
  DATEADD(MINUTE, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 200000);

-- Index sur la colonne de watermark : la requête incrémentale ne scanne plus toute la table (A1)
CREATE INDEX ix_abonnes_modifie_le  ON dbo.abonnes (modifie_le);
CREATE INDEX ix_locations_modifie_le ON dbo.locations (modifie_le);
GO
