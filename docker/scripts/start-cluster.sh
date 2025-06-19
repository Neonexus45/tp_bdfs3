#!/bin/bash

echo "🚀 Démarrage du cluster lakehouse Docker (MySQL externe)..."

# Chargement des variables d'environnement
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "❌ Fichier .env non trouvé!"
    exit 1
fi

# Vérification de la connectivité MySQL
echo "🔍 Vérification connectivité MySQL externe..."
if ! mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} -e "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ Impossible de se connecter au serveur MySQL externe"
    echo "Vérifiez les variables MYSQL_* dans le fichier .env"
    echo "Host: ${MYSQL_HOST}:${MYSQL_PORT}"
    echo "User: ${MYSQL_USER}"
    exit 1
fi

echo "✅ Connexion MySQL réussie!"

# Création des bases de données si nécessaire
echo "📊 Création des bases de données MySQL..."
mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} < scripts/setup-mysql-tables.sql

if [ $? -eq 0 ]; then
    echo "✅ Bases de données créées avec succès!"
else
    echo "❌ Erreur lors de la création des bases de données"
    exit 1
fi

# Nettoyage des conteneurs existants
echo "🧹 Nettoyage des conteneurs existants..."
docker-compose down -v 2>/dev/null

# Construction des images personnalisées
echo "🔨 Construction de l'image MLflow..."
docker-compose build mlflow

# Démarrage des services Docker
echo "🐳 Démarrage des conteneurs Docker..."
docker-compose up -d

# Attendre que les services soient prêts
echo "⏳ Attente initialisation des services..."
echo "   - NameNode..."
sleep 30

# Vérification que NameNode est prêt
while ! docker exec namenode hdfs dfsadmin -report > /dev/null 2>&1; do
    echo "   - NameNode pas encore prêt, attente 10 secondes..."
    sleep 10
done

echo "✅ NameNode prêt!"

# Initialisation HDFS
echo "📁 Initialisation HDFS..."
docker exec namenode hdfs dfsadmin -safemode leave 2>/dev/null
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod 777 /user/hive/warehouse

# Création répertoires projet
echo "📂 Création des répertoires de données..."
docker exec namenode hdfs dfs -mkdir -p /data/bronze
docker exec namenode hdfs dfs -mkdir -p /data/silver  
docker exec namenode hdfs dfs -mkdir -p /data/gold
docker exec namenode hdfs dfs -mkdir -p /data/raw
docker exec namenode hdfs dfs -mkdir -p /data/processed

# Permissions
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /user

echo "✅ HDFS initialisé avec succès!"

# Attendre que Spark soit prêt
echo "⏳ Attente de Spark Master..."
sleep 15

# Attendre que Hive soit prêt
echo "⏳ Attente de Hive Metastore..."
sleep 20

# Attendre que MLflow soit prêt
echo "⏳ Attente de MLflow..."
sleep 10

# Vérification des services
echo "🔍 Vérification des services..."

# Test HDFS
if docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; then
    echo "✅ HDFS opérationnel"
else
    echo "⚠️  HDFS non accessible"
fi

# Test Spark
if curl -s http://localhost:8080 > /dev/null; then
    echo "✅ Spark Master opérationnel"
else
    echo "⚠️  Spark Master non accessible"
fi

# Test Hive
if docker exec hive-metastore /opt/hive/bin/schematool -dbType mysql -info > /dev/null 2>&1; then
    echo "✅ Hive Metastore opérationnel"
else
    echo "⚠️  Hive Metastore non accessible"
fi

# Test MLflow
if curl -s http://localhost:5000 > /dev/null; then
    echo "✅ MLflow opérationnel"
else
    echo "⚠️  MLflow non accessible"
fi

echo ""
echo "🎉 Cluster lakehouse démarré avec MySQL externe!"
echo ""
echo "🌐 Interfaces Web disponibles:"
echo "   - HDFS NameNode:        http://localhost:9870"
echo "   - Yarn ResourceManager: http://localhost:8088"
echo "   - Spark Master:         http://localhost:8080"
echo "   - Spark Worker:         http://localhost:8081"
echo "   - Hive Server2:         http://localhost:10002"
echo "   - MLflow:               http://localhost:5000"
echo ""
echo "🔗 Connexions services:"
echo "   - HDFS:                 hdfs://localhost:9000"
echo "   - Spark Master:         spark://localhost:7077"
echo "   - Hive Metastore:       thrift://localhost:9083"
echo "   - HiveServer2:          jdbc:hive2://localhost:10000"
echo ""
echo "📊 Bases de données MySQL créées:"
echo "   - accidents_db:         Tables Gold Layer"
echo "   - hive_metastore:       Métadonnées Hive"
echo "   - mlflow_db:            Tracking MLflow"
echo ""
echo "📁 Répertoires HDFS créés:"
echo "   - /data/bronze:         Données brutes"
echo "   - /data/silver:         Données nettoyées"
echo "   - /data/gold:           Données agrégées"
echo "   - /user/hive/warehouse: Entrepôt Hive"
echo ""
echo "🚀 Le cluster est prêt pour le traitement des données!"