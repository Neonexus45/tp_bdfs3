@echo off
echo 🚀 Démarrage du cluster lakehouse Docker (MySQL externe)...

REM Chargement des variables d'environnement
if not exist .env (
    echo ❌ Fichier .env non trouvé!
    exit /b 1
)

REM Lecture des variables depuis .env (simplifiée pour Windows)
for /f "usebackq tokens=1,2 delims==" %%a in (".env") do (
    if not "%%a"=="" if not "%%a:~0,1%"=="#" set %%a=%%b
)

echo ✅ Variables d'environnement chargées

REM Note: La vérification MySQL nécessite le client MySQL installé
echo 🔍 Vérification de la connectivité MySQL...
echo Note: Assurez-vous que votre serveur MySQL externe est accessible

REM Nettoyage des conteneurs existants
echo 🧹 Nettoyage des conteneurs existants...
docker-compose down -v 2>nul

REM Construction des images personnalisées
echo 🔨 Construction de l'image MLflow...
docker-compose build mlflow

REM Démarrage des services Docker
echo 🐳 Démarrage des conteneurs Docker...
docker-compose up -d

REM Attendre que les services soient prêts
echo ⏳ Attente initialisation des services...
echo    - NameNode...
timeout /t 30 /nobreak >nul

REM Vérification que NameNode est prêt
:check_namenode
docker exec namenode hdfs dfsadmin -report >nul 2>&1
if errorlevel 1 (
    echo    - NameNode pas encore prêt, attente 10 secondes...
    timeout /t 10 /nobreak >nul
    goto check_namenode
)

echo ✅ NameNode prêt!

REM Initialisation HDFS
echo 📁 Initialisation HDFS...
docker exec namenode hdfs dfsadmin -safemode leave 2>nul
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod 777 /user/hive/warehouse

REM Création répertoires projet
echo 📂 Création des répertoires de données...
docker exec namenode hdfs dfs -mkdir -p /data/bronze
docker exec namenode hdfs dfs -mkdir -p /data/silver
docker exec namenode hdfs dfs -mkdir -p /data/gold
docker exec namenode hdfs dfs -mkdir -p /data/raw
docker exec namenode hdfs dfs -mkdir -p /data/processed

REM Permissions
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /user

echo ✅ HDFS initialisé avec succès!

REM Attendre que les autres services soient prêts
echo ⏳ Attente des autres services...
timeout /t 45 /nobreak >nul

REM Vérification des services
echo 🔍 Vérification des services...

REM Test HDFS
docker exec namenode hdfs dfs -ls / >nul 2>&1
if not errorlevel 1 (
    echo ✅ HDFS opérationnel
) else (
    echo ⚠️  HDFS non accessible
)

echo.
echo 🎉 Cluster lakehouse démarré avec MySQL externe!
echo.
echo 🌐 Interfaces Web disponibles:
echo    - HDFS NameNode:        http://localhost:9870
echo    - Yarn ResourceManager: http://localhost:8088
echo    - Spark Master:         http://localhost:8080
echo    - Spark Worker:         http://localhost:8081
echo    - Hive Server2:         http://localhost:10002
echo    - MLflow:               http://localhost:5000
echo.
echo 🔗 Connexions services:
echo    - HDFS:                 hdfs://localhost:9000
echo    - Spark Master:         spark://localhost:7077
echo    - Hive Metastore:       thrift://localhost:9083
echo    - HiveServer2:          jdbc:hive2://localhost:10000
echo.
echo 📊 Bases de données MySQL à configurer:
echo    - accidents_db:         Tables Gold Layer
echo    - hive_metastore:       Métadonnées Hive
echo    - mlflow_db:            Tracking MLflow
echo.
echo 📁 Répertoires HDFS créés:
echo    - /data/bronze:         Données brutes
echo    - /data/silver:         Données nettoyées
echo    - /data/gold:           Données agrégées
echo    - /user/hive/warehouse: Entrepôt Hive
echo.
echo 🚀 Le cluster est prêt pour le traitement des données!
echo.
echo 💡 Pour créer les tables MySQL, exécutez:
echo    mysql -h [HOST] -u [USER] -p[PASSWORD] ^< scripts\setup-mysql-tables.sql
echo.
echo 💡 Pour vérifier la santé du cluster:
echo    docker-compose ps
echo    docker-compose logs -f