#!/bin/bash

echo "🛑 Arrêt du cluster lakehouse Docker..."

echo "🐳 Arrêt des conteneurs Docker..."
docker-compose down

echo "✅ Cluster lakehouse arrêté!"
echo ""
echo "💡 Pour redémarrer le cluster:"
echo "   cd docker && ./scripts/start-cluster.sh"
echo ""
echo "💡 Pour nettoyer complètement (volumes + données):"
echo "   docker-compose down -v"
echo "   docker system prune -f"