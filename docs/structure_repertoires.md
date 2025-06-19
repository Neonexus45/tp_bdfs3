# Structure des Répertoires et Organisation des Fichiers

## 📁 Arborescence Complète du Projet

```
lakehouse_accidents_us/
├── .env                              # Configuration environnement
├── .gitignore                        # Fichiers à ignorer par Git
├── requirements.txt                  # Dépendances Python
├── setup.py                         # Installation package
├── README.md                        # Documentation projet
├── Dockerfile                       # Conteneurisation (optionnel)
├── docker-compose.yml               # Orchestration services
│
├── config/                          # Configuration centralisée
│   ├── __init__.py
│   ├── config_manager.py            # Gestion configuration centralisée
│   ├── spark_config.py              # Configuration Spark optimisée
│   ├── database_schemas.py          # Schémas MySQL et Hive
│   └── logging_config.yaml          # Configuration logging structuré
│
├── src/                             # Code source principal
│   ├── __init__.py
│   │
│   ├── common/                      # Composants réutilisables
│   │   ├── __init__.py
│   │   ├── base_spark_app.py        # Classe abstraite applications Spark
│   │   ├── structured_logger.py     # Logging structuré
│   │   ├── data_validator.py        # Validation données et schémas
│   │   ├── spark_optimizer.py       # Optimisations Spark
│   │   ├── database_manager.py      # Gestion connexions MySQL
│   │   └── exceptions.py            # Exceptions personnalisées
│   │
│   ├── applications/                # 4 Applications Spark principales
│   │   ├── __init__.py
│   │   │
│   │   ├── feeder/                  # Application d'ingestion
│   │   │   ├── __init__.py
│   │   │   ├── feeder_app.py        # Application principale ingestion
│   │   │   ├── csv_ingester.py      # Ingestion CSV batch
│   │   │   ├── stream_ingester.py   # Ingestion streaming
│   │   │   ├── data_generator.py    # Générateur données temps réel
│   │   │   ├── schema_validator.py  # Validation schéma 47 colonnes
│   │   │   └── partitioner.py       # Partitioning date/état
│   │   │
│   │   ├── preprocessor/            # Application de preprocessing
│   │   │   ├── __init__.py
│   │   │   ├── preprocessor_app.py  # Application principale preprocessing
│   │   │   ├── data_cleaner.py      # Nettoyage données
│   │   │   ├── feature_engineer.py  # Feature engineering
│   │   │   ├── weather_aggregator.py # Agrégations météorologiques
│   │   │   ├── infrastructure_analyzer.py # Analyse infrastructure
│   │   │   ├── temporal_analyzer.py  # Patterns temporels
│   │   │   ├── geographic_analyzer.py # Hotspots géographiques
│   │   │   └── hive_table_creator.py # Création tables Hive
│   │   │
│   │   ├── datamart/               # Application datamart
│   │   │   ├── __init__.py
│   │   │   ├── datamart_app.py      # Application principale datamart
│   │   │   ├── kpi_calculator.py    # Calcul KPIs métier
│   │   │   ├── hotspots_generator.py # Génération zones à risque
│   │   │   ├── temporal_reporter.py  # Rapports patterns temporels
│   │   │   ├── mysql_exporter.py    # Export vers MySQL Gold
│   │   │   └── aggregation_engine.py # Moteur d'agrégation
│   │   │
│   │   └── ml_training/             # Application ML
│   │       ├── __init__.py
│   │       ├── ml_training_app.py   # Application principale ML
│   │       ├── feature_selector.py  # Sélection features
│   │       ├── model_trainer.py     # Entraînement modèles
│   │       ├── model_evaluator.py   # Évaluation performance
│   │       ├── prediction_service.py # Service prédictions
│   │       ├── mlflow_integration.py # Intégration MLflow
│   │       └── model_registry.py    # Registre des modèles
│   │
│   ├── api/                         # API FastAPI
│   │   ├── __init__.py
│   │   ├── main.py                  # Point d'entrée FastAPI
│   │   ├── dependencies.py          # Dépendances communes
│   │   │
│   │   ├── routers/                 # Routeurs par domaine métier
│   │   │   ├── __init__.py
│   │   │   ├── accidents.py         # Endpoints données accidents
│   │   │   ├── hotspots.py          # Endpoints zones à risque
│   │   │   ├── kpis.py              # Endpoints KPIs métier
│   │   │   ├── predictions.py       # Endpoints prédictions ML
│   │   │   └── health.py            # Endpoints santé système
│   │   │
│   │   ├── models/                  # Modèles Pydantic
│   │   │   ├── __init__.py
│   │   │   ├── request_models.py    # Modèles requêtes
│   │   │   ├── response_models.py   # Modèles réponses
│   │   │   └── base_models.py       # Modèles de base
│   │   │
│   │   ├── services/                # Services métier API
│   │   │   ├── __init__.py
│   │   │   ├── accident_service.py  # Service accidents
│   │   │   ├── hotspot_service.py   # Service hotspots
│   │   │   ├── kpi_service.py       # Service KPIs
│   │   │   └── prediction_service.py # Service prédictions
│   │   │
│   │   └── middleware/              # Middlewares
│   │       ├── __init__.py
│   │       ├── rate_limiting.py    # Limitation de débit
│   │       └── cors.py             # Configuration CORS
│   │
│   └── utils/                       # Utilitaires transversaux
│       ├── __init__.py
│       ├── date_utils.py            # Utilitaires dates et temps
│       ├── geo_utils.py             # Utilitaires géographiques
│       ├── spark_utils.py           # Utilitaires Spark
│       ├── validation_utils.py      # Utilitaires validation
│       ├── file_utils.py            # Utilitaires fichiers
│       └── constants.py             # Constantes projet
│
├── data/                            # Données du projet
│   ├── raw/                         # Données brutes
│   │   └── US_Accidents_March23.csv # Dataset principal
│   ├── bronze/                      # Couche Bronze (sera créée)
│   ├── silver/                      # Couche Silver (sera créée)
│   ├── gold/                        # Couche Gold (sera créée)
│   └── models/                      # Modèles ML sauvegardés
│
├── scripts/                         # Scripts de déploiement et maintenance
│   ├── deployment/
│   │   ├── deploy_applications.sh   # Déploiement applications Spark
│   │   ├── setup_environment.sh     # Configuration environnement
│   │   └── start_services.sh        # Démarrage services
│   ├── database/
│   │   ├── create_mysql_tables.sql  # Création tables MySQL
│   │   ├── create_hive_tables.hql   # Création tables Hive
│   │   └── setup_indexes.sql        # Création index optimisés
│   ├── monitoring/
│   │   ├── health_check.py          # Vérification santé système
│   │   └── performance_monitor.py   # Monitoring performance
│   └── data_generation/
│       ├── generate_sample_data.py  # Génération données test
│       └── simulate_streaming.py    # Simulation streaming
│
├── tests/                           # Tests automatisés
│   ├── __init__.py
│   ├── conftest.py                  # Configuration pytest
│   │
│   ├── unit/                        # Tests unitaires
│   │   ├── __init__.py
│   │   ├── common/
│   │   │   ├── test_data_validator.py
│   │   │   ├── test_spark_optimizer.py
│   │   │   └── test_structured_logger.py
│   │   ├── applications/
│   │   │   ├── test_feeder_app.py
│   │   │   ├── test_preprocessor_app.py
│   │   │   ├── test_datamart_app.py
│   │   │   └── test_ml_training_app.py
│   │   ├── api/
│   │   │   ├── test_accident_endpoints.py
│   │   │   ├── test_hotspot_endpoints.py
│   │   │   └── test_prediction_endpoints.py
│   │   └── utils/
│   │       ├── test_date_utils.py
│   │       └── test_geo_utils.py
│   │
│   ├── integration/                 # Tests d'intégration
│   │   ├── __init__.py
│   │   ├── test_end_to_end_pipeline.py
│   │   ├── test_api_database_integration.py
│   │   └── test_spark_hive_integration.py
│   │
│   ├── performance/                 # Tests de performance
│   │   ├── __init__.py
│   │   ├── test_spark_performance.py
│   │   └── test_api_performance.py
│   │
│   └── fixtures/                    # Données de test
│       ├── sample_accidents.csv
│       ├── test_config.env
│       └── mock_responses.json
│
├── docs/                            # Documentation technique
│   ├── architecture_systeme_global.md
│   ├── design_patterns_classes.md
│   ├── structure_repertoires.md
│   ├── configuration_management.md
│   ├── database_schema.md
│   ├── spark_optimization.md
│   ├── api_design.md
│   ├── logging_monitoring.md
│   ├── deployment_testing.md
│   ├── user_guide.md
│   └── troubleshooting.md
│
├── monitoring/                      # Monitoring et observabilité
│   ├── __init__.py
│   ├── metrics/
│   │   ├── spark_metrics.py         # Métriques Spark
│   │   ├── api_metrics.py           # Métriques API
│   │   └── business_metrics.py      # Métriques métier
│   ├── alerts/
│   │   ├── alert_manager.py         # Gestionnaire d'alertes
│   │   └── notification_service.py  # Service notifications
│   └── dashboards/
│       ├── grafana_config.json      # Configuration Grafana
│       └── prometheus_config.yml    # Configuration Prometheus
│
└── notebooks/                       # Notebooks d'analyse (optionnel)
    ├── data_exploration.ipynb       # Exploration données
    ├── model_analysis.ipynb         # Analyse modèles ML
    └── performance_analysis.ipynb   # Analyse performance
```

## 🎯 Principes d'Organisation

### Séparation des Responsabilités
- **`src/common/`** : Composants réutilisables par toutes les applications
- **`src/applications/`** : Applications Spark spécialisées par domaine
- **`src/api/`** : Interface REST avec séparation routers/services/models
- **`src/utils/`** : Utilitaires transversaux sans dépendances métier

### Convention de Nommage
- **Fichiers** : snake_case (ex: `data_validator.py`)
- **Classes** : PascalCase (ex: `DataValidator`)
- **Méthodes/Variables** : snake_case (ex: `validate_schema`)
- **Constantes** : UPPER_SNAKE_CASE (ex: `MAX_BATCH_SIZE`)

### Structure Modulaire
- Chaque module a sa responsabilité unique
- Dépendances claires et minimales
- Facilité de test et maintenance
- Extensibilité pour nouvelles fonctionnalités

## 📦 Gestion des Dépendances

### requirements.txt
```txt
# Spark et Big Data
pyspark==3.4.0
py4j==0.10.9.7

# Base de données
pymysql==1.0.3
sqlalchemy==2.0.15
hive-thrift-py==0.7.0

# API et Web
fastapi==0.100.0
uvicorn==0.22.0
pydantic==2.0.0

# Machine Learning
scikit-learn==1.3.0
mlflow==2.4.0
numpy==1.24.0
pandas==2.0.0

# Utilitaires
python-dotenv==1.0.0
pyyaml==6.0
structlog==23.1.0
click==8.1.0

# Tests
pytest==7.4.0
pytest-cov==4.1.0
pytest-mock==3.11.0

# Développement
black==23.3.0
flake8==6.0.0
mypy==1.4.0
```

### setup.py
```python
from setuptools import setup, find_packages

setup(
    name="lakehouse-accidents-us",
    version="1.0.0",
    description="Architecture Lakehouse pour l'analyse des accidents US",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.4.0",
        "fastapi>=0.100.0",
        "pymysql>=1.0.3",
        # ... autres dépendances
    ],
    entry_points={
        "console_scripts": [
            "feeder=applications.feeder.feeder_app:main",
            "preprocessor=applications.preprocessor.preprocessor_app:main",
            "datamart=applications.datamart.datamart_app:main",
            "ml-training=applications.ml_training.ml_training_app:main",
        ],
    },
)
```

## 🔧 Configuration par Environnement

### Structure de Configuration
```
config/
├── base.yaml              # Configuration de base
├── development.yaml       # Surcharges développement
├── staging.yaml          # Surcharges staging
├── production.yaml       # Surcharges production
└── secrets/              # Secrets par environnement
    ├── dev_secrets.env
    ├── staging_secrets.env
    └── prod_secrets.env
```

## 📋 Standards de Développement

### Formatage du Code
- **Black** : Formatage automatique Python
- **Flake8** : Vérification style et qualité
- **MyPy** : Vérification types statiques

### Documentation
- **Docstrings** : Format Google style
- **Type Hints** : Obligatoires pour toutes les fonctions publiques
- **README** : Documentation utilisateur et développeur

### Tests
- **Coverage** : Minimum 80% de couverture
- **Tests unitaires** : Un fichier de test par module
- **Tests d'intégration** : Validation des flux complets
- **Tests de performance** : Validation des optimisations Spark

## 🚀 Déploiement et Maintenance

### Scripts de Déploiement
- **Automatisation** : Scripts bash pour déploiement
- **Validation** : Vérification santé après déploiement
- **Rollback** : Procédure de retour arrière

### Monitoring
- **Logs structurés** : Format JSON pour parsing automatique
- **Métriques** : Collecte via Prometheus/Grafana
- **Alertes** : Notification automatique des problèmes

Cette structure garantit une organisation claire, une maintenance facilitée et une évolutivité optimale pour le projet lakehouse d'analyse des accidents US.