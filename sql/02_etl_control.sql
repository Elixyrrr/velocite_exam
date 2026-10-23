-- VéloCité — contrôle ETL (watermark, journal) + droits de l'identité managée ADF
-- Exécuté bloc par bloc dans le Query editor (connexion Microsoft Entra).

CREATE SCHEMA etl;

CREATE TABLE etl.control (
  table_name    SYSNAME   NOT NULL PRIMARY KEY,
  watermark_col SYSNAME   NULL,                    -- NULL = copie complète (stations)
  last_value    DATETIME2 NOT NULL DEFAULT '1900-01-01',
  lookback_days INT       NOT NULL DEFAULT 7,      -- R2 : fenêtre de corrections
  enabled       BIT       NOT NULL DEFAULT 1
);
INSERT INTO etl.control (table_name, watermark_col) VALUES
  ('abonnes','modifie_le'), ('locations','modifie_le'), ('stations', NULL);

CREATE TABLE etl.run_log (
  log_id BIGINT IDENTITY PRIMARY KEY, pipeline_run NVARCHAR(64), run_date DATE,
  table_name SYSNAME, status NVARCHAR(20), rows_read BIGINT NULL, rows_copied BIGINT NULL,
  wm_from DATETIME2 NULL, wm_to DATETIME2 NULL, error_message NVARCHAR(4000) NULL,
  logged_at DATETIME2 DEFAULT SYSUTCDATETIME()
);

CREATE OR ALTER PROCEDURE etl.usp_set_watermark @table_name SYSNAME, @new_value DATETIME2
AS
BEGIN
  SET NOCOUNT ON;
  UPDATE etl.control SET last_value = @new_value
  WHERE table_name = @table_name AND @new_value > last_value;
END;

CREATE OR ALTER PROCEDURE etl.usp_log_run
  @pipeline_run NVARCHAR(64), @run_date DATE, @table_name SYSNAME, @status NVARCHAR(20),
  @rows_read BIGINT = NULL, @rows_copied BIGINT = NULL,
  @wm_from DATETIME2 = NULL, @wm_to DATETIME2 = NULL, @error_message NVARCHAR(4000) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  INSERT INTO etl.run_log (pipeline_run, run_date, table_name, status, rows_read, rows_copied, wm_from, wm_to, error_message)
  VALUES (@pipeline_run, @run_date, @table_name, @status, @rows_read, @rows_copied, @wm_from, @wm_to, LEFT(@error_message, 4000));
END;

-- Identité managée ADF : lecture + exécution des procédures etl, rien d'autre (moindre privilège)
CREATE USER [adf-velocite-dev] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [adf-velocite-dev];
GRANT EXECUTE ON SCHEMA::etl TO [adf-velocite-dev];

-- Requête source de la Copy locations (contenu dynamique ADF) :
-- @concat('SELECT * FROM dbo.locations WHERE modifie_le > DATEADD(DAY, -7, CAST(''',
--   activity('LKP_WM_LOCATIONS').output.firstRow.last_value, ''' AS DATETIME2)) AND modifie_le <= CAST(''',
--   formatDateTime(pipeline().TriggerTime, 'yyyy-MM-ddTHH:mm:ss'), ''' AS DATETIME2)')
