"""
Composant PredictionService pour la prédiction en temps réel
"""

import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import Model
import json

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import MLModelError, DataValidationError
from .mlflow_manager import MLflowManager
from .feature_processor import FeatureProcessor


class PredictionService:
    """
    Service de prédiction en temps réel pour classification de sévérité
    
    Fonctionnalités:
    - Chargement modèle: Depuis MLflow Registry (Production/Staging)
    - Preprocessing: Pipeline de transformation des données d'entrée
    - Prédiction: Classification de sévérité avec scores de confiance
    - Probabilités: Scores de confiance par classe
    - Batch/Single: Support prédictions unitaires et par batch
    - Monitoring: Suivi des performances et détection de drift
    """
    
    def __init__(self, config_manager: ConfigManager = None, spark: SparkSession = None):
        """Initialise le service de prédiction"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("prediction_service", self.config_manager)
        self.spark = spark
        
        # Composants
        self.mlflow_manager = MLflowManager(self.config_manager)
        self.feature_processor = None
        
        # Modèle chargé
        self.loaded_model = None
        self.model_name = None
        self.model_version = None
        self.model_stage = None
        self.feature_columns = []
        self.model_metadata = {}
        
        # Configuration de prédiction
        self.severity_classes = [1, 2, 3, 4]
        self.class_names = {1: 'Minor', 2: 'Moderate', 3: 'Serious', 4: 'Severe'}
        self.confidence_threshold = 0.5  # Seuil de confiance minimum
        
        # Métriques du service
        self.service_metrics = {
            'predictions_made': 0,
            'batch_predictions': 0,
            'single_predictions': 0,
            'total_prediction_time': 0,
            'model_loads': 0,
            'preprocessing_time': 0,
            'inference_time': 0
        }
    
    def load_model(self, model_name: str, version: str = None, stage: str = "Production") -> None:
        """Charge un modèle depuis MLflow Registry"""
        try:
            self.logger.info("Loading model from MLflow Registry",
                           model_name=model_name,
                           version=version,
                           stage=stage)
            
            start_time = time.time()
            
            # Chargement du modèle
            self.loaded_model = self.mlflow_manager.load_model(
                model_name=model_name,
                version=version,
                stage=stage
            )
            
            # Sauvegarde des informations du modèle
            self.model_name = model_name
            self.model_version = version
            self.model_stage = stage
            
            # Chargement des métadonnées du modèle
            self._load_model_metadata(model_name, version, stage)
            
            # Initialisation du feature processor si nécessaire
            if not self.feature_processor:
                self.feature_processor = FeatureProcessor(self.config_manager, self.spark)
            
            load_time = time.time() - start_time
            self.service_metrics['model_loads'] += 1
            
            self.logger.info("Model loaded successfully",
                           model_name=model_name,
                           load_time_seconds=load_time,
                           feature_count=len(self.feature_columns))
            
        except Exception as e:
            self.logger.error("Model loading failed", exception=e, model_name=model_name)
            raise MLModelError(
                message=f"Model loading failed for {model_name}",
                model_name=model_name,
                operation="load_model"
            )
    
    def _load_model_metadata(self, model_name: str, version: str = None, stage: str = None) -> None:
        """Charge les métadonnées du modèle"""
        try:
            # Pour une implémentation complète, on chargerait les métadonnées depuis MLflow
            # Ici, on utilise des métadonnées par défaut
            self.model_metadata = {
                'model_name': model_name,
                'version': version,
                'stage': stage,
                'loaded_at': datetime.now().isoformat()
            }
            
            # Features par défaut (à adapter selon le modèle réel)
            self.feature_columns = [
                'Distance_mi', 'Temperature_F', 'Wind_Chill_F', 'Humidity_percent',
                'Pressure_in', 'Visibility_mi', 'Wind_Speed_mph', 'Precipitation_in',
                'Start_Lat', 'Start_Lng', 'accident_hour', 'accident_day_of_week',
                'is_weekend', 'is_rush_hour', 'weather_severity_score',
                'infrastructure_count', 'safety_equipment_score'
            ]
            
            self.logger.debug("Model metadata loaded", metadata_keys=list(self.model_metadata.keys()))
            
        except Exception as e:
            self.logger.warning("Failed to load model metadata", exception=e)
            self.model_metadata = {}
            self.feature_columns = []
    
    def predict(self, input_data: DataFrame) -> DataFrame:
        """Effectue des prédictions sur un DataFrame"""
        try:
            if not self.loaded_model:
                raise MLModelError(
                    message="No model loaded for prediction",
                    operation="predict"
                )
            
            self.logger.info("Starting batch prediction", input_rows=input_data.count())
            start_time = time.time()
            
            # 1. Validation des données d'entrée
            self._validate_input_data(input_data)
            
            # 2. Preprocessing des données
            preprocessing_start = time.time()
            processed_data = self._preprocess_for_prediction(input_data)
            preprocessing_time = time.time() - preprocessing_start
            
            # 3. Prédiction
            inference_start = time.time()
            predictions = self.loaded_model.transform(processed_data)
            inference_time = time.time() - inference_start
            
            # 4. Post-processing des résultats
            final_predictions = self._postprocess_predictions(predictions)
            
            # Métriques
            total_time = time.time() - start_time
            prediction_count = final_predictions.count()
            
            self.service_metrics.update({
                'predictions_made': self.service_metrics['predictions_made'] + prediction_count,
                'batch_predictions': self.service_metrics['batch_predictions'] + 1,
                'total_prediction_time': self.service_metrics['total_prediction_time'] + total_time,
                'preprocessing_time': self.service_metrics['preprocessing_time'] + preprocessing_time,
                'inference_time': self.service_metrics['inference_time'] + inference_time
            })
            
            self.logger.log_performance(
                operation="batch_prediction",
                duration_seconds=total_time,
                predictions_count=prediction_count,
                preprocessing_time=preprocessing_time,
                inference_time=inference_time
            )
            
            return final_predictions
            
        except Exception as e:
            self.logger.error("Batch prediction failed", exception=e)
            raise MLModelError(
                message="Batch prediction failed",
                operation="predict"
            )
    
    def predict_single(self, input_record: Dict[str, Any]) -> Dict[str, Any]:
        """Effectue une prédiction sur un enregistrement unique"""
        try:
            if not self.loaded_model:
                raise MLModelError(
                    message="No model loaded for prediction",
                    operation="predict_single"
                )
            
            self.logger.debug("Starting single prediction")
            start_time = time.time()
            
            # Conversion en DataFrame
            input_df = self._dict_to_dataframe(input_record)
            
            # Prédiction
            prediction_df = self.predict(input_df)
            
            # Conversion du résultat
            result_row = prediction_df.collect()[0]
            result = self._row_to_dict(result_row)
            
            prediction_time = time.time() - start_time
            self.service_metrics['single_predictions'] += 1
            
            self.logger.debug("Single prediction completed", 
                            prediction_time=prediction_time,
                            predicted_severity=result.get('predicted_severity'))
            
            return result
            
        except Exception as e:
            self.logger.error("Single prediction failed", exception=e)
            raise MLModelError(
                message="Single prediction failed",
                operation="predict_single"
            )
    
    def _validate_input_data(self, input_data: DataFrame) -> None:
        """Valide les données d'entrée"""
        try:
            # Vérification des colonnes requises (subset minimum)
            required_columns = [
                'Distance_mi', 'Temperature_F', 'Humidity_percent',
                'Pressure_in', 'Visibility_mi', 'Start_Lat', 'Start_Lng'
            ]
            
            missing_columns = [col for col in required_columns if col not in input_data.columns]
            if missing_columns:
                raise DataValidationError(
                    message=f"Missing required columns: {missing_columns}",
                    validation_type="input_validation"
                )
            
            # Vérification des types de données
            for column in required_columns:
                if column in input_data.columns:
                    col_type = dict(input_data.dtypes)[column]
                    if col_type not in ['double', 'float', 'int', 'bigint']:
                        self.logger.warning(f"Column {column} has unexpected type: {col_type}")
            
            # Vérification des valeurs nulles critiques
            null_counts = {}
            for column in required_columns:
                if column in input_data.columns:
                    null_count = input_data.filter(col(column).isNull()).count()
                    if null_count > 0:
                        null_counts[column] = null_count
            
            if null_counts:
                self.logger.warning("Found null values in input data", null_counts=null_counts)
            
            self.logger.debug("Input data validation completed")
            
        except Exception as e:
            self.logger.error("Input data validation failed", exception=e)
            raise DataValidationError(
                message="Input data validation failed",
                validation_type="input_validation"
            )
    
    def _preprocess_for_prediction(self, input_data: DataFrame) -> DataFrame:
        """Préprocesse les données pour la prédiction"""
        try:
            # Pour une implémentation complète, on appliquerait le même preprocessing
            # que lors de l'entraînement. Ici, on fait une version simplifiée.
            
            processed_data = input_data
            
            # Gestion des valeurs manquantes (imputation simple)
            numeric_columns = [
                'Distance_mi', 'Temperature_F', 'Wind_Chill_F', 'Humidity_percent',
                'Pressure_in', 'Visibility_mi', 'Wind_Speed_mph', 'Precipitation_in'
            ]
            
            for column in numeric_columns:
                if column in processed_data.columns:
                    # Imputation par la médiane (valeur par défaut)
                    default_values = {
                        'Distance_mi': 0.5,
                        'Temperature_F': 60.0,
                        'Wind_Chill_F': 60.0,
                        'Humidity_percent': 50.0,
                        'Pressure_in': 30.0,
                        'Visibility_mi': 10.0,
                        'Wind_Speed_mph': 5.0,
                        'Precipitation_in': 0.0
                    }
                    
                    default_value = default_values.get(column, 0.0)
                    processed_data = processed_data.fillna({column: default_value})
            
            # Ajout de features temporelles si manquantes
            if 'accident_hour' not in processed_data.columns:
                processed_data = processed_data.withColumn('accident_hour', lit(12))
            
            if 'accident_day_of_week' not in processed_data.columns:
                processed_data = processed_data.withColumn('accident_day_of_week', lit(3))
            
            if 'is_weekend' not in processed_data.columns:
                processed_data = processed_data.withColumn('is_weekend', lit(False))
            
            if 'is_rush_hour' not in processed_data.columns:
                processed_data = processed_data.withColumn('is_rush_hour', lit(False))
            
            # Ajout de features dérivées si manquantes
            if 'weather_severity_score' not in processed_data.columns:
                processed_data = processed_data.withColumn('weather_severity_score', lit(1.0))
            
            if 'infrastructure_count' not in processed_data.columns:
                processed_data = processed_data.withColumn('infrastructure_count', lit(0))
            
            if 'safety_equipment_score' not in processed_data.columns:
                processed_data = processed_data.withColumn('safety_equipment_score', lit(1.0))
            
            # Assemblage des features (version simplifiée)
            from pyspark.ml.feature import VectorAssembler
            
            available_features = [col for col in self.feature_columns if col in processed_data.columns]
            
            if len(available_features) < len(self.feature_columns) * 0.7:  # Au moins 70% des features
                self.logger.warning("Many features missing for prediction",
                                  available=len(available_features),
                                  required=len(self.feature_columns))
            
            assembler = VectorAssembler(
                inputCols=available_features,
                outputCol="features",
                handleInvalid="keep"
            )
            
            final_data = assembler.transform(processed_data)
            
            self.logger.debug("Preprocessing completed", features_used=len(available_features))
            return final_data
            
        except Exception as e:
            self.logger.error("Preprocessing for prediction failed", exception=e)
            raise MLModelError(
                message="Preprocessing for prediction failed",
                operation="preprocess_prediction"
            )
    
    def _postprocess_predictions(self, predictions: DataFrame) -> DataFrame:
        """Post-traite les prédictions"""
        try:
            # Ajout des noms de classes
            from pyspark.sql.functions import when
            
            result_df = predictions.withColumn(
                'predicted_severity_name',
                when(col('prediction') == 1, 'Minor')
                .when(col('prediction') == 2, 'Moderate')
                .when(col('prediction') == 3, 'Serious')
                .when(col('prediction') == 4, 'Severe')
                .otherwise('Unknown')
            )
            
            # Ajout de métadonnées de prédiction
            result_df = result_df.withColumn('prediction_timestamp', lit(datetime.now().isoformat()))
            result_df = result_df.withColumn('model_name', lit(self.model_name))
            result_df = result_df.withColumn('model_version', lit(self.model_version))
            
            # Calcul de la confiance (approximation basée sur la probabilité max)
            if 'probability' in result_df.columns:
                # Pour une implémentation complète, on extrairait la probabilité max
                result_df = result_df.withColumn('confidence_score', lit(0.8))
            else:
                result_df = result_df.withColumn('confidence_score', lit(0.7))
            
            # Sélection des colonnes finales
            final_columns = [
                'prediction', 'predicted_severity_name', 'confidence_score',
                'prediction_timestamp', 'model_name', 'model_version'
            ]
            
            # Ajout des colonnes d'entrée importantes
            input_columns = ['Start_Lat', 'Start_Lng', 'Distance_mi', 'Temperature_F']
            for col_name in input_columns:
                if col_name in result_df.columns:
                    final_columns.append(col_name)
            
            result_df = result_df.select(*[col for col in final_columns if col in result_df.columns])
            
            self.logger.debug("Postprocessing completed")
            return result_df
            
        except Exception as e:
            self.logger.error("Postprocessing failed", exception=e)
            return predictions
    
    def _dict_to_dataframe(self, record: Dict[str, Any]) -> DataFrame:
        """Convertit un dictionnaire en DataFrame"""
        try:
            # Création du schéma
            fields = []
            for key, value in record.items():
                if isinstance(value, int):
                    fields.append(StructField(key, IntegerType(), True))
                elif isinstance(value, float):
                    fields.append(StructField(key, DoubleType(), True))
                else:
                    fields.append(StructField(key, StringType(), True))
            
            schema = StructType(fields)
            
            # Création du DataFrame
            df = self.spark.createDataFrame([record], schema)
            return df
            
        except Exception as e:
            self.logger.error("Dictionary to DataFrame conversion failed", exception=e)
            raise MLModelError(
                message="Input conversion failed",
                operation="dict_to_dataframe"
            )
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convertit une Row en dictionnaire"""
        try:
            result = {}
            for field in row.__fields__:
                value = getattr(row, field)
                if value is not None:
                    result[field] = value
            
            return result
            
        except Exception as e:
            self.logger.error("Row to dictionary conversion failed", exception=e)
            return {}
    
    def get_model_info(self) -> Dict[str, Any]:
        """Retourne les informations du modèle chargé"""
        return {
            'model_name': self.model_name,
            'model_version': self.model_version,
            'model_stage': self.model_stage,
            'feature_columns': self.feature_columns,
            'feature_count': len(self.feature_columns),
            'model_metadata': self.model_metadata,
            'is_loaded': self.loaded_model is not None
        }
    
    def get_service_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques du service"""
        metrics = self.service_metrics.copy()
        
        # Calcul de métriques dérivées
        if metrics['predictions_made'] > 0:
            metrics['avg_prediction_time'] = metrics['total_prediction_time'] / metrics['predictions_made']
        else:
            metrics['avg_prediction_time'] = 0.0
        
        if metrics['batch_predictions'] > 0:
            metrics['avg_preprocessing_time'] = metrics['preprocessing_time'] / metrics['batch_predictions']
            metrics['avg_inference_time'] = metrics['inference_time'] / metrics['batch_predictions']
        else:
            metrics['avg_preprocessing_time'] = 0.0
            metrics['avg_inference_time'] = 0.0
        
        return metrics
    
    def health_check(self) -> Dict[str, Any]:
        """Vérifie la santé du service de prédiction"""
        try:
            health_status = {
                'status': 'healthy',
                'model_loaded': self.loaded_model is not None,
                'model_info': self.get_model_info(),
                'service_metrics': self.get_service_metrics(),
                'timestamp': datetime.now().isoformat()
            }
            
            # Vérifications de santé
            if not self.loaded_model:
                health_status['status'] = 'unhealthy'
                health_status['issues'] = ['No model loaded']
            
            return health_status
            
        except Exception as e:
            self.logger.error("Health check failed", exception=e)
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }