"""
Application MLTRAINING pour l'entraînement des modèles de classification de sévérité

Cette application constitue la couche ML de l'architecture Medallion, transformant
les données enrichies de la couche Silver en modèles prédictifs performants.

Composants principaux:
- MLTrainingApp: Application principale orchestrant le pipeline ML
- FeatureProcessor: Préparation des features pour ML
- ModelTrainer: Entraînement de multiples modèles ML
- ModelEvaluator: Évaluation complète des modèles
- HyperparameterTuner: Optimisation des hyperparamètres
- MLflowManager: Gestion MLflow (tracking, registry)
- PredictionService: Service de prédiction en temps réel

Architecture:
Bronze (FEEDER) → Silver (PREPROCESSOR) → Gold (DATAMART) → ML (MLTRAINING)
     ↓                    ↓                     ↓              ↓
  HDFS Parquet      →  Hive ORC         →   MySQL        →  MLflow
  Données brutes    →  Features enrichies → Analytics    →  Modèles ML
"""

from .mltraining_app import MLTrainingApp
from .feature_processor import FeatureProcessor
from .model_trainer import ModelTrainer
from .model_evaluator import ModelEvaluator
from .hyperparameter_tuner import HyperparameterTuner
from .mlflow_manager import MLflowManager
from .prediction_service import PredictionService

__version__ = "1.0.0"
__author__ = "Lakehouse Accidents Team"

__all__ = [
    "MLTrainingApp",
    "FeatureProcessor", 
    "ModelTrainer",
    "ModelEvaluator",
    "HyperparameterTuner",
    "MLflowManager",
    "PredictionService"
]

# Configuration par défaut pour l'application
DEFAULT_CONFIG = {
    "target_column": "Severity",
    "severity_classes": [1, 2, 3, 4],
    "class_names": {1: "Minor", 2: "Moderate", 3: "Serious", 4: "Severe"},
    "train_ratio": 0.70,
    "validation_ratio": 0.15,
    "test_ratio": 0.15,
    "random_seed": 42,
    "cv_folds": 3,
    "confidence_threshold": 0.5
}

# Modèles supportés
SUPPORTED_MODELS = [
    "random_forest",
    "gradient_boosting", 
    "logistic_regression",
    "decision_tree"
]

# Métriques d'évaluation
EVALUATION_METRICS = [
    "accuracy",
    "f1_score_macro",
    "f1_score_micro", 
    "precision_macro",
    "recall_macro",
    "auc_macro"
]

# Features par catégorie
FEATURE_CATEGORIES = {
    "numeric": [
        "Distance_mi", "Temperature_F", "Wind_Chill_F", "Humidity_percent",
        "Pressure_in", "Visibility_mi", "Wind_Speed_mph", "Precipitation_in",
        "Start_Lat", "Start_Lng"
    ],
    "categorical": [
        "Source", "State", "Weather_Condition", "Sunrise_Sunset",
        "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"
    ],
    "boolean": [
        "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
        "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
        "Traffic_Signal", "Turning_Loop"
    ],
    "temporal": [
        "accident_hour", "accident_day_of_week", "accident_month",
        "accident_season", "is_weekend", "is_rush_hour"
    ],
    "weather": [
        "weather_category", "weather_severity_score", 
        "visibility_category", "temperature_category"
    ],
    "geographic": [
        "distance_to_city_center", "urban_rural_classification",
        "state_region", "population_density_category"
    ],
    "infrastructure": [
        "infrastructure_count", "safety_equipment_score", "traffic_control_type"
    ]
}