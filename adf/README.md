# Export Azure Data Factory

Déposer ici le contenu dézippé de : ADF Studio → Manage → ARM template → Export ARM template.

Pipeline `P01_RAW` :
- LKP_WM_LOCATIONS → COPY_LOCATIONS_TO_RAW → SP_SET_WM_LOCATIONS → SP_LOG_LOCATIONS
- LKP_WM_ABONNES → COPY_ABONNES_TO_RAW → SP_SET_WM_ABONNES → SP_LOG_ABONNES
- COPY_STATIONS_TO_RAW (référentiel de 50 lignes, sans colonne modifie_le : copie complète)

Retries 3 × 60 s sur les Copy. Linked services en identité managée système (aucun secret).
Sink : raw/sql/<table>/ingest_date=yyyy-MM-dd/<RunId>.parquet
