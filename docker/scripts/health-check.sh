#!/bin/bash

echo "🔍 Vérification de la santé du cluster lakehouse..."
echo "=================================================="

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Fonction pour tester un service
check_service() {
    local service_name=$1
    local test_command=$2
    local description=$3
    
    echo -n "🔍 $description... "
    
    if eval $test_command > /dev/null 2>&1; then
        echo -e "${GREEN}✅ OK${NC}"
        return 0
    else
        echo -e "${RED}❌ ERREUR${NC}"
        return 1
    fi
}

# Fonction pour tester une URL
check_url() {
    local service_name=$1
    local url=$2
    local description=$3
    
    echo -n "🌐 $description... "
    
    if curl -s --connect-timeout 5 $url > /dev/null; then
        echo -e "${GREEN}✅ OK${NC}"
        return 0
    else
        echo -e "${RED}❌ ERREUR${NC}"
        return 1
    fi
}

# Variables de comptage
total_checks=0
passed_checks=0

echo ""
echo "🐳 CONTENEURS DOCKER"
echo "===================="

# Vérification des conteneurs
containers=("namenode" "datanode" "resourcemanager" "nodemanager" "spark-master" "spark-worker" "hive-metastore" "hiveserver2" "mlflow")

for container in "${containers[@]}"; do
    total_checks=$((total_checks + 1))
    if check_service "$container" "docker ps --format 'table {{.Names}}' | grep -q $container" "Conteneur $container"; then
        passed_checks=$((passed_checks + 1))
    fi
done

echo ""
echo "🌐 INTERFACES WEB"
echo "================="

# Vérification des interfaces web
total_checks=$((total_checks + 1))
if check_url "hdfs" "http://localhost:9870" "HDFS NameNode (port 9870)"; then
    passed_checks=$((passed_checks + 1))
fi

total_checks=$((total_checks + 1))
if check_url "yarn" "http://localhost:8088" "Yarn ResourceManager (port 8088)"; then
    passed_checks=$((passed_checks + 1))
fi

total_checks=$((total_checks + 1))
if check_url "spark-master" "http://localhost:8080" "Spark Master (port 8080)"; then
    passed_checks=$((passed_checks + 1))
fi

total_checks=$((total_checks + 1))
if check_url "spark-worker" "http://localhost:8081" "Spark Worker (port 8081)"; then
    passed_checks=$((passed_checks + 1))
fi

total_checks=$((total_checks + 1))
if check_url "hive" "http://localhost:10002" "Hive Server2 (port 10002)"; then
    passed_checks=$((passed_checks + 1))
fi

total_checks=$((total_checks + 1))
if check_url "mlflow" "http://localhost:5000" "MLflow (port 5000)"; then
    passed_checks=$((passed_checks + 1))
fi

echo ""
echo "🔧 SERVICES FONCTIONNELS"
echo "========================"

# Test HDFS
total_checks=$((total_checks + 1))
if check_service "hdfs" "docker exec namenode hdfs dfs -ls /" "HDFS - Listage racine"; then
    passed_checks=$((passed_checks + 1))
fi

# Test répertoires HDFS
total_checks=$((total_checks + 1))
if check_service "hdfs-dirs" "docker exec namenode hdfs dfs -ls /data" "HDFS - Répertoires de données"; then
    passed_checks=$((passed_checks + 1))
fi

# Test Hive Metastore
total_checks=$((total_checks + 1))
if check_service "hive-metastore" "docker exec hive-metastore /opt/hive/bin/schematool -dbType mysql -info" "Hive Metastore - Schéma"; then
    passed_checks=$((passed_checks + 1))
fi

# Test connexion MySQL (si variables disponibles)
if [ ! -z "$MYSQL_HOST" ]; then
    total_checks=$((total_checks + 1))
    if check_service "mysql" "mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} -e 'SELECT 1;'" "MySQL externe - Connexion"; then
        passed_checks=$((passed_checks + 1))
    fi
fi

echo ""
echo "📊 RÉSUMÉ"
echo "========="

percentage=$((passed_checks * 100 / total_checks))

if [ $percentage -eq 100 ]; then
    echo -e "${GREEN}🎉 Tous les services sont opérationnels! ($passed_checks/$total_checks)${NC}"
elif [ $percentage -ge 80 ]; then
    echo -e "${YELLOW}⚠️  La plupart des services fonctionnent ($passed_checks/$total_checks - $percentage%)${NC}"
else
    echo -e "${RED}❌ Plusieurs services ont des problèmes ($passed_checks/$total_checks - $percentage%)${NC}"
fi

echo ""
echo "🔗 LIENS UTILES"
echo "==============="
echo "HDFS NameNode:        http://localhost:9870"
echo "Yarn ResourceManager: http://localhost:8088"
echo "Spark Master:         http://localhost:8080"
echo "Spark Worker:         http://localhost:8081"
echo "Hive Server2:         http://localhost:10002"
echo "MLflow:               http://localhost:5000"

echo ""
echo "📋 COMMANDES UTILES"
echo "==================="
echo "Logs conteneur:       docker logs <nom_conteneur>"
echo "Shell conteneur:      docker exec -it <nom_conteneur> bash"
echo "Redémarrer service:   docker-compose restart <service>"
echo "Voir tous les logs:   docker-compose logs -f"

# Code de sortie basé sur le pourcentage de réussite
if [ $percentage -ge 80 ]; then
    exit 0
else
    exit 1
fi