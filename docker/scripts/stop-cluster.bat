@echo off
echo 🛑 Arrêt du cluster lakehouse Docker...

REM Arrêt des conteneurs
echo 🐳 Arrêt des conteneurs Docker...
docker-compose down

echo ✅ Cluster lakehouse arrêté!
echo.
echo 💡 Pour redémarrer le cluster:
echo    cd docker ^&^& scripts\start-cluster.bat
echo.
echo 💡 Pour nettoyer complètement (volumes + données):
echo    docker-compose down -v
echo    docker system prune -f