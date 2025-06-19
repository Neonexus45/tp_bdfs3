# Application MLTRAINING

Application d'entraînement de modèles ML pour la classification de sévérité des accidents de la route aux États-Unis.

## Vue d'ensemble

L'application MLTRAINING constitue la couche ML de l'architecture Medallion, transformant les données enrichies de la couche Silver en modèles prédictifs performants pour classifier la sévérité des accidents (1-4).

### Architecture Medallion - Couche ML

```
Bronze (FEEDER) → Silver (PREPROCESSOR) → Gold (DATAMART) → ML (MLTRAINING)
     ↓                    ↓                     ↓              ↓
  HDFS Parquet      →  Hive ORC         →   MySQL        →  MLflow
  Données brutes    →  Features enrichies → Analytics    →  Modèles ML
```

## Fonctionnalités principales

### 🔧 Préparation des Features ML
- **Source** : Couche Silver (Hive table `accidents_clean`)
- **Features** : 47 colonnes US-Accidents + features enrichies (temporelles, météo, géographiques, infrastructure)
- **Preprocessing** : Scaling, encoding, feature selection, gestion outliers
- **Pipeline** : Reproductible et versionné

### 🤖 Entraînement Multi-Modèles
- **RandomForestClassifier** : Robuste et interprétable
- **GradientBoostingClassifier** : Performance élevée
- **LogisticRegression** : Baseline rapide
- **DecisionTreeClassifier** : Natif Spark

### 📊 Évaluation Complète
- **Métriques globales** : Accuracy, F1-score macro/micro, AUC
- **Métriques par classe** : Precision, Recall, F1 pour chaque sévérité
- **Confusion Matrix** : Analyse détaillée des erreurs
- **Feature Importance** : Ranking des variables prédictives

### ⚙️ Optimisation Hyperparamètres
- **Grid Search** : Recherche exhaustive
- **Random Search** : Recherche aléatoire efficace
- **Cross-validation** : Validation croisée k-fold
- **Optimisation** : Basée sur F1-score macro

### 📈 MLflow Integration
- **Tracking** : Expériences, paramètres, métriques
- **Model Registry** : Versioning et staging (Staging/Production)
- **Artifacts** : Modèles, visualisations, métadonnées

### 🔮 Service de Prédiction
- **Chargement** : Modèles depuis MLflow Registry
- **Preprocessing** : Pipeline de transformation
- **Prédiction** : Classification avec scores de confiance
- **API** : Support batch et prédictions unitaires

## Structure du projet

```
src/applications/mltraining/
├── __init__.py                    # Package initialization
├── mltraining_app.py             # Application principale
├── feature_processor.py          # Préparation features ML
├── model_trainer.py              # Entraînement modèles
├── model_evaluator.py            # Évaluation et métriques
├── hyperparameter_tuner.py       # Tuning hyperparamètres
├── mlflow_manager.py             # Gestion MLflow
├── prediction_service.py         # Service de prédiction
├── test_mltraining.py            # Tests complets
└── README.md                     # Documentation
```

## Configuration

### Variables d'environnement

```bash
# MLflow
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=accidents-severity-prediction
MLFLOW_ARTIFACT_ROOT=/mlflow/artifacts

# Hive
HIVE_METASTORE_URI=thrift://localhost:9083
HIVE_DATABASE=accidents_warehouse
HIVE_TABLE_ACCIDENTS_CLEAN=accidents_clean

# Spark ML
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=2
```

### Configuration modèles

```python
# Paramètres RandomForest
rf_params = {
    'numTrees': [50, 100, 200],
    'maxDepth': [10, 15, 20],
    'minInstancesPerNode': [1, 5, 10],
    'subsamplingRate': [0.8, 1.0]
}

# Paramètres GradientBoosting
gb_params = {
    'maxIter': [50, 100, 150],
    'maxDepth': [5, 10, 15],
    'stepSize': [0.01, 0.1, 0.2],
    'subsamplingRate': [0.8, 1.0]
}
```

## Utilisation

### 1. Entraînement complet

```python
from src.applications.mltraining.mltraining_app import MLTrainingApp

# Initialisation
ml_app = MLTrainingApp()
ml_app.initialize_components()

# Pipeline ML complet
result = ml_app.run_ml_pipeline()

print(f"Meilleur modèle: {result['model_info']['name']}")
print(f"Accuracy: {result['metrics']['accuracy']:.4f}")
print(f"F1-Score: {result['metrics']['f1_score_macro']:.4f}")
```

### 2. Entraînement modèle spécifique

```python
from src.applications.mltraining.model_trainer import ModelTrainer

trainer = ModelTrainer()

# Entraînement Random Forest
rf_result = trainer.train_single_model(
    model_name='random_forest',
    train_df=train_data,
    validation_df=val_data,
    feature_columns=feature_list,
    target_column='Severity'
)

print(f"F1-Score: {rf_result['validation_metrics']['f1_score_macro']}")
```

### 3. Optimisation hyperparamètres

```python
from src.applications.mltraining.hyperparameter_tuner import HyperparameterTuner

tuner = HyperparameterTuner()

# Grid search
best_model, tuning_results = tuner.tune_model(
    model_name='random_forest',
    train_df=train_data,
    validation_df=val_data,
    feature_columns=feature_list,
    target_column='Severity',
    tuning_method='grid_search'
)

print(f"Meilleurs paramètres: {tuning_results['best_params']}")
print(f"Score optimisé: {tuning_results['best_score']}")
```

### 4. Évaluation détaillée

```python
from src.applications.mltraining.model_evaluator import ModelEvaluator

evaluator = ModelEvaluator()

# Évaluation complète
evaluation = evaluator.evaluate_model(
    model=trained_model,
    test_df=test_data,
    feature_columns=feature_list,
    target_column='Severity',
    model_name='random_forest_tuned'
)

# Métriques par classe
for class_name, metrics in evaluation['class_metrics'].items():
    print(f"{class_name}: Precision={metrics['precision']:.3f}, "
          f"Recall={metrics['recall']:.3f}, F1={metrics['f1_score']:.3f}")

# Matrice de confusion
confusion = evaluation['confusion_matrix']
print(f"Total échantillons: {confusion['total_samples']}")
```

### 5. Service de prédiction

```python
from src.applications.mltraining.prediction_service import PredictionService

# Initialisation du service
predictor = PredictionService()
predictor.load_model('accidents_severity_random_forest', stage='Production')

# Prédiction unitaire
accident_data = {
    'Distance_mi': 0.5,
    'Temperature_F': 65.0,
    'Humidity_percent': 60.0,
    'Start_Lat': 40.7128,
    'Start_Lng': -74.0060
}

prediction = predictor.predict_single(accident_data)
print(f"Sévérité prédite: {prediction['predicted_severity_name']}")
print(f"Confiance: {prediction['confidence_score']:.3f}")

# Prédiction batch
batch_predictions = predictor.predict(input_dataframe)
```

## Features utilisées

### 47 Colonnes originales US-Accidents

```python
# Variables numériques
numeric_features = [
    'Distance_mi', 'Temperature_F', 'Wind_Chill_F', 'Humidity_percent',
    'Pressure_in', 'Visibility_mi', 'Wind_Speed_mph', 'Precipitation_in',
    'Start_Lat', 'Start_Lng'
]

# Variables catégorielles
categorical_features = [
    'Source', 'State', 'Weather_Condition', 'Sunrise_Sunset',
    'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight'
]

# Variables booléennes infrastructure (13 colonnes)
boolean_features = [
    'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
    'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming',
    'Traffic_Signal', 'Turning_Loop'
]
```

### Features enrichies par PREPROCESSOR

```python
# Features temporelles
temporal_features = [
    'accident_hour', 'accident_day_of_week', 'accident_month',
    'accident_season', 'is_weekend', 'is_rush_hour'
]

# Features météorologiques
weather_features = [
    'weather_category', 'weather_severity_score', 
    'visibility_category', 'temperature_category'
]

# Features géographiques
geo_features = [
    'distance_to_city_center', 'urban_rural_classification',
    'state_region', 'population_density_category'
]

# Features infrastructure agrégées
infra_features = [
    'infrastructure_count', 'safety_equipment_score', 'traffic_control_type'
]
```

## Métriques et évaluation

### Métriques globales

- **Accuracy** : Pourcentage de prédictions correctes
- **F1-Score Macro** : Moyenne des F1 par classe (métrique principale)
- **F1-Score Micro** : F1 global pondéré
- **AUC Macro** : Moyenne des AUC par classe

### Métriques par classe de sévérité

| Sévérité | Description | Métriques |
|----------|-------------|-----------|
| 1 | Minor | Precision, Recall, F1-Score, Support |
| 2 | Moderate | Precision, Recall, F1-Score, Support |
| 3 | Serious | Precision, Recall, F1-Score, Support |
| 4 | Severe | Precision, Recall, F1-Score, Support |

### Analyses avancées

- **Confusion Matrix** : Matrice d'erreurs détaillée
- **Feature Importance** : Ranking des variables les plus importantes
- **ROC/AUC** : Courbes ROC pour chaque classe
- **Calibration** : Qualité des probabilités prédites

## MLflow Integration

### Tracking des expériences

```python
# Logging automatique
with mlflow.start_run():
    mlflow.log_params(model_params)
    mlflow.log_metrics(evaluation_metrics)
    mlflow.spark.log_model(model, "model")
    mlflow.log_artifacts("plots/")
```

### Model Registry

- **Staging** : Modèles en test
- **Production** : Modèles déployés
- **Archived** : Anciens modèles

### Artifacts sauvegardés

- Modèles Spark ML
- Métadonnées des features
- Matrices de confusion
- Graphiques de performance
- Configuration des hyperparamètres

## Optimisations Spark ML

### Configuration mémoire

```python
spark_config = {
    'spark.driver.memory': '4g',
    'spark.executor.memory': '2g',
    'spark.executor.cores': '2',
    'spark.sql.adaptive.enabled': 'true',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
}
```

### Pipeline optimisé

- **Feature Caching** : Cache des features transformées
- **Broadcast Variables** : Tables de référence pour encoding
- **Partitioning** : Données équilibrées par partition
- **Vectorisation** : Assemblage efficace des features

## Tests

### Exécution des tests

```bash
# Tests unitaires
python -m pytest src/applications/mltraining/test_mltraining.py -v

# Tests spécifiques
python -m pytest src/applications/mltraining/test_mltraining.py::TestMLTrainingApp -v

# Tests avec couverture
python -m pytest src/applications/mltraining/test_mltraining.py --cov=src.applications.mltraining
```

### Types de tests

- **Tests unitaires** : Chaque composant individuellement
- **Tests d'intégration** : Pipeline ML complet
- **Tests de performance** : Métriques et temps d'exécution
- **Tests de validation** : Qualité des données et modèles

## Monitoring et logging

### Métriques applicatives

```json
{
  "operation": "ml_training_pipeline",
  "duration_seconds": 1847.3,
  "models_trained": 4,
  "best_model": "random_forest",
  "best_f1_score": 0.8734,
  "features_processed": 67,
  "training_samples": 5390000,
  "validation_samples": 1155000,
  "test_samples": 1155000
}
```

### Logging structuré

- **JSON** : Format structuré pour analyse
- **Niveaux** : DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Contexte** : Métadonnées enrichies (modèle, étape, métriques)
- **Rotation** : Gestion automatique des fichiers de log

## Déploiement

### Prérequis

- Apache Spark 3.4+
- MLflow 2.4+
- Hive Metastore
- MySQL 8.0+
- Python 3.8+

### Installation

```bash
# Installation des dépendances
pip install -r requirements.txt

# Configuration des variables d'environnement
cp .env.example .env
# Éditer .env avec vos configurations

# Initialisation de la base de données
python scripts/init_ml_tables.py
```

### Exécution

```bash
# Entraînement complet
python -m src.applications.mltraining.mltraining_app

# Avec configuration personnalisée
python -m src.applications.mltraining.mltraining_app --config custom_config.yaml
```

## Troubleshooting

### Problèmes courants

1. **Erreur MLflow** : Vérifier `MLFLOW_TRACKING_URI`
2. **Mémoire Spark** : Augmenter `spark.driver.memory`
3. **Données manquantes** : Vérifier table Hive `accidents_clean`
4. **Performance** : Optimiser partitioning et cache

### Logs utiles

```bash
# Logs application
tail -f /var/log/lakehouse-accidents/mltraining.log

# Logs Spark
tail -f $SPARK_HOME/logs/spark-*.log

# Logs MLflow
tail -f /var/log/mlflow/mlflow.log
```

## Contribution

### Standards de code

- **PEP 8** : Style Python
- **Type hints** : Annotations de type
- **Docstrings** : Documentation des fonctions
- **Tests** : Couverture > 80%

### Workflow

1. Fork du repository
2. Branche feature (`git checkout -b feature/nouvelle-fonctionnalite`)
3. Commits (`git commit -am 'Ajout nouvelle fonctionnalité'`)
4. Push (`git push origin feature/nouvelle-fonctionnalite`)
5. Pull Request

## Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

## Support

- **Documentation** : [Wiki du projet](wiki-url)
- **Issues** : [GitHub Issues](issues-url)
- **Contact** : equipe-ml@lakehouse-accidents.com