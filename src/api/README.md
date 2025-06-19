# API d'Analyse des Accidents US

API REST FastAPI pour l'analyse des données d'accidents de la route aux États-Unis. Cette API expose les données de la couche Gold du lakehouse et fournit des endpoints pour consulter les accidents, KPIs, hotspots et prédictions ML.

## 🚀 Démarrage Rapide

### Prérequis

- Python 3.8+
- MySQL (couche Gold configurée)
- Dépendances Python (voir `requirements.txt`)

### Installation

```bash
# Installation des dépendances
pip install -r requirements.txt

# Configuration de la base de données
# Assurez-vous que la couche Gold MySQL est accessible
```

### Lancement de l'API

```bash
# Depuis la racine du projet
python -m src.api.main

# Ou avec uvicorn directement
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

L'API sera accessible sur `http://localhost:8000`

## 📚 Documentation

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI JSON**: `http://localhost:8000/openapi.json`

## 🛠️ Endpoints Principaux

### Santé et Information

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/` | GET | Informations de base de l'API |
| `/health` | GET | Vérification de santé (API + DB) |
| `/info` | GET | Informations détaillées sur l'API |

### Accidents (`/api/v1/accidents`)

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/api/v1/accidents` | GET | Liste des accidents avec pagination et filtres |
| `/api/v1/accidents/stats` | GET | Statistiques générales des accidents |
| `/api/v1/accidents/by-state` | GET | Accidents groupés par état |
| `/api/v1/accidents/by-severity` | GET | Accidents groupés par sévérité |
| `/api/v1/accidents/timeline` | GET | Évolution temporelle des accidents |

#### Paramètres de filtrage

- `state`: Code d'état (ex: CA, TX, FL)
- `severity`: Niveau de sévérité (1-4)
- `start_date` / `end_date`: Période temporelle
- `weather_condition`: Condition météorologique
- `page` / `size`: Pagination

### KPIs (`/api/v1/kpis`)

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/api/v1/kpis` | GET | KPIs généraux de sécurité routière |
| `/api/v1/kpis/by-state` | GET | KPIs par état |
| `/api/v1/kpis/trends` | GET | Tendances temporelles des KPIs |
| `/api/v1/kpis/weather-impact` | GET | Impact météorologique |
| `/api/v1/kpis/severity-analysis` | GET | Analyse de sévérité |

### Hotspots (`/api/v1/hotspots`)

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/api/v1/hotspots` | GET | Zones dangereuses identifiées |
| `/api/v1/hotspots/geographic-analysis` | GET | Analyse géographique détaillée |
| `/api/v1/hotspots/risk-assessment` | GET | Évaluation des risques par zone |

### Prédictions ML (`/api/v1/predict`)

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/api/v1/predict` | POST | Prédiction de sévérité d'accident |
| `/api/v1/predict/batch` | POST | Prédictions en lot |
| `/api/v1/predict/validate` | POST | Validation des données d'entrée |
| `/api/v1/predict/models` | GET | Modèles ML disponibles |
| `/api/v1/predict/health` | GET | Santé du service ML |
| `/api/v1/predict/examples` | GET | Exemples de données |

## 📊 Exemples d'Utilisation

### Récupérer les accidents en Californie

```bash
curl "http://localhost:8000/api/v1/accidents?state=CA&page=1&size=10"
```

### Obtenir les KPIs généraux

```bash
curl "http://localhost:8000/api/v1/kpis"
```

### Prédire la sévérité d'un accident

```bash
curl -X POST "http://localhost:8000/api/v1/predict" \
  -H "Content-Type: application/json" \
  -d '{
    "state": "CA",
    "start_lat": 34.0522,
    "start_lng": -118.2437,
    "weather_condition": "Clear",
    "temperature_fahrenheit": 72.0,
    "humidity_percent": 65.0,
    "visibility_miles": 10.0,
    "accident_hour": 17,
    "accident_day_of_week": 1
  }'
```

### Identifier les hotspots

```bash
curl "http://localhost:8000/api/v1/hotspots?state=CA&min_accidents=50"
```

## 🧪 Tests

### Test automatique de l'API

```bash
# Test complet de tous les endpoints
python src/api/test_api.py

# Test avec URL personnalisée
python src/api/test_api.py --url http://localhost:8000

# Sauvegarder les résultats
python src/api/test_api.py --output test_results.json
```

### Test manuel avec curl

```bash
# Vérification de santé
curl http://localhost:8000/health

# Test d'un endpoint simple
curl http://localhost:8000/api/v1/accidents?page=1&size=5
```

## 🏗️ Architecture

### Structure des Dossiers

```
src/api/
├── main.py                 # Application FastAPI principale
├── test_api.py            # Tests automatisés
├── models/                # Modèles Pydantic
│   ├── accident_models.py
│   ├── kpi_models.py
│   ├── hotspot_models.py
│   ├── prediction_models.py
│   └── response_models.py
├── routers/               # Endpoints FastAPI
│   ├── accidents.py
│   ├── kpis.py
│   ├── hotspots.py
│   └── predictions.py
├── services/              # Logique métier
│   ├── accident_service.py
│   ├── kpi_service.py
│   ├── hotspot_service.py
│   └── prediction_service.py
├── middleware/            # Middleware FastAPI
│   ├── cors_middleware.py
│   └── logging_middleware.py
└── dependencies/          # Dépendances FastAPI
    └── database.py
```

### Couches de l'Architecture

1. **Routers**: Définition des endpoints et validation des entrées
2. **Services**: Logique métier et orchestration
3. **Models**: Modèles Pydantic pour validation et sérialisation
4. **Dependencies**: Injection de dépendances (DB, config)
5. **Middleware**: Traitement transversal (CORS, logging)

## 🔧 Configuration

### Variables d'Environnement

L'API utilise le système de configuration centralisé du projet. Les principales configurations :

```yaml
# config/config.yaml
api:
  host: "0.0.0.0"
  port: 8000
  cors_origins: ["*"]

database:
  gold:
    host: "localhost"
    port: 3306
    database: "lakehouse_gold"
    # ... autres paramètres DB
```

### Configuration CORS

Par défaut, l'API accepte toutes les origines pour faciliter le développement. En production, configurez les origines autorisées :

```python
# Dans cors_middleware.py
allowed_origins = [
    "http://localhost:3000",
    "https://votre-frontend.com"
]
```

## 📈 Monitoring et Logging

### Logs Structurés

L'API génère des logs JSON structurés pour faciliter l'analyse :

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "service": "api",
  "endpoint": "/api/v1/accidents",
  "method": "GET",
  "status_code": 200,
  "response_time_ms": 45,
  "user_agent": "curl/7.68.0"
}
```

### Métriques Disponibles

- Temps de réponse par endpoint
- Taux d'erreur par service
- Utilisation de la base de données
- Métriques des prédictions ML

## 🚨 Gestion d'Erreurs

### Codes de Statut HTTP

- `200`: Succès
- `400`: Données d'entrée invalides
- `404`: Ressource non trouvée
- `422`: Erreur de validation Pydantic
- `500`: Erreur interne du serveur
- `503`: Service indisponible (DB inaccessible)

### Format des Erreurs

```json
{
  "error": "Description de l'erreur",
  "detail": "Détails techniques",
  "timestamp": "2024-01-15T10:30:00Z",
  "path": "/api/v1/accidents"
}
```

## 🔮 Prédictions ML

### Modèles Supportés

- **Random Forest**: Modèle par défaut, bon équilibre précision/performance
- **XGBoost**: Haute précision pour prédictions critiques
- **Logistic Regression**: Modèle simple et rapide

### Données d'Entrée pour Prédictions

#### Champs Requis
- `state`: Code d'état US (2 lettres)
- `start_lat`: Latitude de l'accident
- `start_lng`: Longitude de l'accident

#### Champs Optionnels (améliorent la précision)
- `weather_condition`: Condition météorologique
- `temperature_fahrenheit`: Température en Fahrenheit
- `humidity_percent`: Humidité en pourcentage
- `visibility_miles`: Visibilité en miles
- `wind_speed_mph`: Vitesse du vent en mph
- `accident_hour`: Heure de l'accident (0-23)
- `accident_day_of_week`: Jour de la semaine (0-6)

### Validation des Données

Utilisez l'endpoint `/api/v1/predict/validate` pour vérifier la qualité de vos données avant prédiction.

## 🛡️ Sécurité

### Version Projet Scolaire

Cette version est simplifiée pour un projet scolaire et **ne contient pas** :
- Authentification JWT
- Rate limiting
- Chiffrement des données sensibles
- Audit des accès

### Pour une Version Production

Pour un déploiement en production, ajoutez :
- Authentification et autorisation
- Rate limiting par IP/utilisateur
- Chiffrement HTTPS obligatoire
- Validation stricte des entrées
- Audit et monitoring de sécurité

## 🤝 Contribution

### Ajout d'un Nouvel Endpoint

1. Créer le modèle Pydantic dans `models/`
2. Implémenter la logique dans `services/`
3. Ajouter l'endpoint dans `routers/`
4. Mettre à jour les tests dans `test_api.py`
5. Documenter dans ce README

### Standards de Code

- Type hints obligatoires
- Docstrings pour toutes les fonctions publiques
- Tests unitaires pour la logique métier
- Logs structurés pour le debugging

## 📞 Support

Pour les questions techniques ou problèmes :

1. Vérifiez les logs de l'API
2. Testez la connectivité à la base de données
3. Utilisez l'endpoint `/health` pour diagnostiquer
4. Consultez la documentation Swagger

## 📝 Changelog

### Version 1.0.0
- API REST complète pour accidents, KPIs, hotspots
- Intégration prédictions ML
- Documentation Swagger automatique
- Tests automatisés
- Version simplifiée pour projet scolaire (sans authentification)