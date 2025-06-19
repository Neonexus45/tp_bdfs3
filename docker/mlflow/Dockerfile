FROM python:3.9-slim

WORKDIR /mlflow

# Installation dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Installation dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Création des répertoires
RUN mkdir -p /mlflow/artifacts /mlflow/work

# Configuration MLflow
EXPOSE 5000

# Script de démarrage
COPY start-mlflow.sh /mlflow/
RUN chmod +x /mlflow/start-mlflow.sh

CMD ["/mlflow/start-mlflow.sh"]