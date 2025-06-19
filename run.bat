@echo off
echo ========================================
echo    PIPELINE ETL LAKEHOUSE US-ACCIDENTS
echo ========================================
echo.

echo.
echo [2/6] Execution FEEDER (Bronze Layer)...
echo Ingestion des donnees brutes vers HDFS Parquet...
python -m src.applications.feeder.feeder_app
if %ERRORLEVEL% neq 0 (
    echo ERREUR: Echec du FEEDER
    pause
    exit /b 1
)

echo.
echo [3/6] Execution PREPROCESSOR (Silver Layer)...
echo Nettoyage et feature engineering vers Hive ORC...
python -m src.applications.preprocessor.preprocessor_app
if %ERRORLEVEL% neq 0 (
    echo ERREUR: Echec du PREPROCESSOR
    pause
    exit /b 1
)

echo.
echo [4/6] Execution DATAMART (Gold Layer)...
echo Agregations business et export vers MySQL...
python -m src.applications.datamart.datamart_app
if %ERRORLEVEL% neq 0 (
    echo ERREUR: Echec du DATAMART
    pause
    exit /b 1
)

echo.
echo [5/6] Execution MLTRAINING (ML Layer)...
echo Entrainement modeles et export vers MLflow...
python -m src.applications.mltraining.mltraining_app
if %ERRORLEVEL% neq 0 (
    echo ERREUR: Echec du MLTRAINING
    pause
    exit /b 1
)

echo.
echo [6/6] Demarrage de l'API FastAPI...
start "API FastAPI" python -m src.api.main

echo.
echo ========================================
echo    PIPELINE ETL TERMINE AVEC SUCCES !
echo ========================================
echo.
echo Interfaces disponibles:
echo - HDFS NameNode: http://localhost:9870
echo - Yarn ResourceManager: http://localhost:8088
echo - Spark Master: http://localhost:8080
echo - MLflow: http://localhost:5000
echo - API Swagger: http://localhost:8000/docs
echo.
echo Demarrage de Streamlit pour visualisation...
cd streamlit
streamlit run app.py