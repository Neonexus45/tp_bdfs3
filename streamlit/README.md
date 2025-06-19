# 📊 Dashboard Streamlit - Lakehouse US-Accidents

## Vue d'ensemble

Cette application Streamlit fournit une interface de visualisation interactive pour explorer les résultats du pipeline lakehouse US-Accidents. Elle se connecte directement à la base de données MySQL (couche Gold) pour afficher les KPIs, analyses et résultats des modèles ML.

## Fonctionnalités

### 🏠 Page d'Accueil
- Vue d'ensemble des métriques générales
- Architecture Medallion visualisée
- Statut du pipeline ETL

### 📊 KPIs de Sécurité
- Top 10 états les plus dangereux
- Taux d'accidents par 100k habitants
- Index de dangerosité par zone
- Distribution des hotspots

### ⏰ Analyse Temporelle
- Évolution des accidents par période
- Analyse saisonnière
- Tendances par état
- Facteurs temporels

### 🗺️ Hotspots Géographiques
- Carte interactive des zones dangereuses
- Top hotspots par état
- Analyse géospatiale
- Corrélations géographiques

### 🤖 Performance ML
- Métriques des modèles (Accuracy, F1-score, etc.)
- Comparaison entre modèles
- Importance des features
- Évolution des performances

### 📈 Données Brutes
- Exploration interactive des données
- Filtres dynamiques
- Statistiques descriptives
- Export des données

## Installation

```bash
# Installation des dépendances
pip install -r requirements.txt

# Démarrage de l'application
streamlit run app.py
```

## Configuration

L'application utilise la configuration MySQL définie dans le fichier `.env` du projet principal :

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=tatane
DB_PASSWORD=tatane
DB_NAME=accidents_db
```

## Prérequis

1. **Pipeline ETL exécuté** : Les applications DATAMART et MLTRAINING doivent avoir été exécutées pour peupler la base MySQL
2. **Base MySQL accessible** : La base de données doit être accessible avec les tables suivantes :
   - `accidents_summary`
   - `kpis_security`
   - `kpis_temporal`
   - `hotspots`
   - `ml_model_performance`

## Utilisation

1. **Démarrer l'infrastructure** :
   ```bash
   cd docker
   ./scripts/start-cluster.sh
   ```

2. **Exécuter le pipeline ETL** :
   ```bash
   ./run.bat  # Windows
   ./run.sh   # Linux/Mac
   ```

3. **Lancer Streamlit** :
   ```bash
   cd streamlit
   streamlit run app.py
   ```

4. **Accéder au dashboard** : http://localhost:8501

## Architecture

```
streamlit/
├── app.py              # Application principale
├── requirements.txt    # Dépendances Python
└── README.md          # Documentation
```

## Fonctionnalités Techniques

- **Connexion MySQL** : Utilise mysql-connector-python
- **Visualisations** : Plotly Express et Graph Objects
- **Interface** : Streamlit avec layout responsive
- **Configuration** : Intégration avec ConfigManager du projet
- **Gestion d'erreurs** : Affichage des erreurs de connexion
- **Performance** : Requêtes optimisées avec LIMIT

## Captures d'Écran

L'application génère automatiquement :
- Graphiques interactifs Plotly
- Cartes géographiques avec hotspots
- Tableaux de données filtrables
- Métriques en temps réel
- Comparaisons de modèles ML

## Dépannage

### Erreur de connexion MySQL
```
Erreur de connexion à MySQL: [Errno] Can't connect to MySQL server
```
**Solution** : Vérifier que MySQL est démarré et accessible avec les paramètres du .env

### Aucune donnée disponible
```
Aucune donnée disponible. Exécutez d'abord le pipeline complet.
```
**Solution** : Exécuter le pipeline ETL avec `./run.bat` ou `./run.sh`

### Erreur d'import
```
ModuleNotFoundError: No module named 'src'
```
**Solution** : Lancer Streamlit depuis le répertoire racine du projet

## Intégration

Cette application Streamlit s'intègre parfaitement avec :
- **Pipeline ETL** : Consomme les données générées
- **API FastAPI** : Peut utiliser les mêmes endpoints
- **Infrastructure Docker** : Utilise la même base MySQL
- **Configuration** : Partage le même ConfigManager