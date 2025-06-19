#!/bin/bash

echo "🚀 Démarrage MLflow Server..."

# Attendre que MySQL soit disponible
echo "⏳ Attente de la disponibilité de MySQL..."
while ! mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} -e "SELECT 1;" > /dev/null 2>&1; do
    echo "MySQL non disponible, attente 5 secondes..."
    sleep 5
done

echo "✅ MySQL disponible, démarrage MLflow..."

# Démarrage du serveur MLflow
mlflow server \
    --backend-store-uri "${MLFLOW_BACKEND_STORE_URI}" \
    --default-artifact-root "${MLFLOW_DEFAULT_ARTIFACT_ROOT}" \
    --host 0.0.0.0 \
    --port 5000