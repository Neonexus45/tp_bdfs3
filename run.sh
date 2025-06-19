#!/bin/bash
echo "========================================"
echo "   PIPELINE ETL LAKEHOUSE US-ACCIDENTS"
echo "========================================"
echo

echo "[1/6] Vérification de l'infrastructure Docker..."
cd docker
./scripts/start-cluster.sh
if [ $? -ne 0 ]; then
    echo "ERREUR: Infrastructure Docker non disponible"
    exit 1
fi
cd ..

echo
echo "[2/6] Exécution FEEDER (Bronze Layer)..."
echo "Ingestion des données brutes vers HDFS Parquet..."
python -m src.applications.feeder.feeder_app
if [ $? -ne 0 ]; then
    echo "ERREUR: Échec du FEEDER"
    exit 1
fi

echo
echo "[3/6] Exécution PREPROCESSOR (Silver Layer)..."
echo "Nettoyage et feature engineering vers Hive ORC..."
python -m src.applications.preprocessor.preprocessor_app
if [ $? -ne 0 ]; then
    echo "ERREUR: Échec du PREPROCESSOR"
    exit 1
fi

echo
echo "[4/6] Exécution DATAMART (Gold Layer)..."
echo "Agrégations business et export vers MySQL..."
python -m src.applications.datamart.datamart_app
if [ $? -ne 0 ]; then
    echo "ERREUR: Échec du DATAMART"
    exit 1
fi

echo
echo "[5/6] Exécution MLTRAINING (ML Layer)..."
echo "Entraînement modèles et export vers MLflow..."
python -m src.applications.mltraining.mltraining_app
if [ $? -ne 0 ]; then
    echo "ERREUR: Échec du MLTRAINING"
    exit 1
fi

echo
echo "[6/6] Démarrage de l'API FastAPI..."
python -m src.api.main &
API_PID=$!

echo
echo "========================================"
echo "   PIPELINE ETL TERMINÉ AVEC SUCCÈS !"
echo "========================================"
echo
echo "Interfaces disponibles:"
echo "- HDFS NameNode: http://localhost:9870"
echo "- Yarn ResourceManager: http://localhost:8088"
echo "- Spark Master: http://localhost:8080"
echo "- MLflow: http://localhost:5000"
echo "- API Swagger: http://localhost:8000/docs"
echo
echo "Démarrage de Streamlit pour visualisation..."
cd streamlit
streamlit run app.py