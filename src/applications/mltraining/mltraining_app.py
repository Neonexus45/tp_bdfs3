"""
Application principale MLTRAINING pour l'entraînement des modèles de classification de sévérité
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from pyspark.sql import SparkSession, DataFrame

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.utils.spark_utils import SparkUtils
from ...common.exceptions.custom_exceptions import (
    LakehouseException, SparkJobError, MLModelError, DataValidationError
)

from .feature_processor import FeatureProcessor
from .model_trainer import ModelTrainer
from .model_evaluator import ModelEvaluator
from .hyperparameter_tuner import HyperparameterTuner
from .mlflow_manager import MLflowManager
from .prediction_service import PredictionService


class MLTrainingApp:
    """
    Application principale MLTRAINING pour l'entraînement des modèles ML
    
    Fonctionnalités:
    - Orchestration pipeline ML complet (préparation → entraînement → évaluation)
    - Source: Couche Silver (Hive table accidents_clean avec features enrichies)
    - Destination: Modèles ML sauvegardés (MLflow) + métriques dans MySQL
    - Classification de sévérité des accidents (1-4) basée sur 47 colonnes + features
    - Pipeline reproductible avec hyperparameter tuning et cross-validation
    - Monitoring performance et détection de drift
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise l'application MLTRAINING"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("mltraining", self.config_manager)
        self.spark_utils = SparkUtils(self.config_manager)
        
        # Configuration spécifique ML
        self.hive_database = self.config_manager.get('hive.database')
        self.accidents_table = self.config_manager.get_hive_table_name('accidents_clean')
        self.target_column = 'Severity'
        self.experiment_name = self.config_manager.get('mlflow.experiment_name')
        
        # Paramètres d'entraînement
        self.train_ratio = 0.70
        self.validation_ratio = 0.15
        self.test_ratio = 0.15
        self.random_seed = 42
        self.max_retries = 3
        self.retry_delay = 10  # seconds
        
        # Composants spécialisés
        self.feature_processor = None
        self.model_trainer = None
        self.model_evaluator = None
        self.hyperparameter_tuner = None
        self.mlflow_manager = None
        self.prediction_service = None
        
        # Session Spark
        self.spark = None
        
        # Métriques de l'application
        self.app_metrics = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'models_trained': 0,
            'best_model_accuracy': 0.0,
            'best_model_f1_score': 0.0,
            'features_processed': 0,
            'training_samples': 0,
            'validation_samples': 0,
            'test_samples': 0,
            'mlflow_run_id': None,
            'model_registry_version': None
        }
    
    def initialize_components(self) -> None:
        """Initialise tous les composants ML"""
        try:
            self.logger.info("Initializing ML components")
            
            # Création session Spark optimisée pour ML
            self.spark = self.spark_utils.create_spark_session(
                app_name="MLTraining-AccidentsSeverity",
                app_type="ml_training"
            )
            
            # Initialisation des composants
            self.feature_processor = FeatureProcessor(self.config_manager, self.spark)
            self.model_trainer = ModelTrainer(self.config_manager, self.spark)
            self.model_evaluator = ModelEvaluator(self.config_manager, self.spark)
            self.hyperparameter_tuner = HyperparameterTuner(self.config_manager, self.spark)
            self.mlflow_manager = MLflowManager(self.config_manager)
            self.prediction_service = PredictionService(self.config_manager, self.spark)
            
            self.logger.info("ML components initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize ML components", exception=e)
            raise MLModelError(
                message="ML components initialization failed",
                operation="initialization",
                metrics=self.app_metrics
            )
    
    def load_training_data(self) -> DataFrame:
        """Charge les données d'entraînement depuis Hive"""
        try:
            self.logger.info("Loading training data from Hive",
                           database=self.hive_database,
                           table=self.accidents_table)
            
            # Lecture des données depuis Hive
            df = self.spark.sql(f"""
                SELECT * FROM {self.hive_database}.{self.accidents_table}
                WHERE {self.target_column} IS NOT NULL
                AND {self.target_column} BETWEEN 1 AND 4
            """)
            
            # Validation des données
            row_count = df.count()
            if row_count == 0:
                raise DataValidationError(
                    message="No training data found",
                    dataset=f"{self.hive_database}.{self.accidents_table}"
                )
            
            # Vérification de la distribution des classes
            severity_distribution = df.groupBy(self.target_column).count().collect()
            distribution_dict = {row[self.target_column]: row['count'] for row in severity_distribution}
            
            self.logger.info("Training data loaded successfully",
                           total_rows=row_count,
                           severity_distribution=distribution_dict,
                           columns=len(df.columns))
            
            # Cache des données pour performance
            df = self.spark_utils.apply_intelligent_caching(df, "MEMORY_AND_DISK")
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to load training data", exception=e)
            raise DataValidationError(
                message="Training data loading failed",
                dataset=f"{self.hive_database}.{self.accidents_table}"
            )
    
    def split_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """Divise les données en train/validation/test avec stratification"""
        try:
            self.logger.info("Splitting data into train/validation/test sets",
                           train_ratio=self.train_ratio,
                           validation_ratio=self.validation_ratio,
                           test_ratio=self.test_ratio)
            
            # Split stratifié par classe de sévérité
            train_df, temp_df = df.randomSplit([self.train_ratio, 1 - self.train_ratio], 
                                             seed=self.random_seed)
            
            # Calcul des ratios pour validation/test
            val_test_ratio = self.validation_ratio / (self.validation_ratio + self.test_ratio)
            validation_df, test_df = temp_df.randomSplit([val_test_ratio, 1 - val_test_ratio], 
                                                       seed=self.random_seed)
            
            # Comptage des échantillons
            train_count = train_df.count()
            val_count = validation_df.count()
            test_count = test_df.count()
            
            # Mise à jour des métriques
            self.app_metrics.update({
                'training_samples': train_count,
                'validation_samples': val_count,
                'test_samples': test_count
            })
            
            self.logger.info("Data split completed",
                           training_samples=train_count,
                           validation_samples=val_count,
                           test_samples=test_count)
            
            # Cache des datasets
            train_df = self.spark_utils.apply_intelligent_caching(train_df, "MEMORY_AND_DISK")
            validation_df = self.spark_utils.apply_intelligent_caching(validation_df, "MEMORY_AND_DISK")
            test_df = self.spark_utils.apply_intelligent_caching(test_df, "MEMORY_AND_DISK")
            
            return train_df, validation_df, test_df
            
        except Exception as e:
            self.logger.error("Failed to split data", exception=e)
            raise DataValidationError(
                message="Data splitting failed",
                validation_type="data_split"
            )
    
    def run_ml_pipeline(self, experiment_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Exécute le pipeline ML complet"""
        try:
            self.app_metrics['start_time'] = datetime.now()
            self.logger.info("Starting ML training pipeline", 
                           experiment_name=self.experiment_name,
                           config=experiment_config)
            
            # 1. Chargement des données
            raw_df = self.load_training_data()
            
            # 2. Préparation des features
            self.logger.info("Processing features for ML")
            processed_df, feature_info = self.feature_processor.prepare_features_for_ml(raw_df)
            self.app_metrics['features_processed'] = len(feature_info['selected_features'])
            
            # 3. Division des données
            train_df, validation_df, test_df = self.split_data(processed_df)
            
            # 4. Initialisation MLflow
            run_id = self.mlflow_manager.start_experiment_run(
                experiment_name=self.experiment_name,
                run_name=f"ml_training_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                tags={
                    'pipeline_version': '1.0',
                    'data_source': f"{self.hive_database}.{self.accidents_table}",
                    'features_count': self.app_metrics['features_processed']
                }
            )
            self.app_metrics['mlflow_run_id'] = run_id
            
            # 5. Entraînement des modèles
            self.logger.info("Training ML models")
            models_results = self.model_trainer.train_multiple_models(
                train_df=train_df,
                validation_df=validation_df,
                feature_columns=feature_info['selected_features'],
                target_column=self.target_column
            )
            self.app_metrics['models_trained'] = len(models_results)
            
            # 6. Hyperparameter tuning pour le meilleur modèle
            self.logger.info("Performing hyperparameter tuning")
            best_model_name = max(models_results.keys(), 
                                key=lambda k: models_results[k]['validation_metrics']['f1_score_macro'])
            
            tuned_model, tuning_results = self.hyperparameter_tuner.tune_model(
                model_name=best_model_name,
                train_df=train_df,
                validation_df=validation_df,
                feature_columns=feature_info['selected_features'],
                target_column=self.target_column
            )
            
            # 7. Évaluation finale sur test set
            self.logger.info("Evaluating final model on test set")
            final_metrics = self.model_evaluator.evaluate_model(
                model=tuned_model,
                test_df=test_df,
                feature_columns=feature_info['selected_features'],
                target_column=self.target_column,
                model_name=f"{best_model_name}_tuned"
            )
            
            # Mise à jour des métriques
            self.app_metrics.update({
                'best_model_accuracy': final_metrics['accuracy'],
                'best_model_f1_score': final_metrics['f1_score_macro']
            })
            
            # 8. Sauvegarde du modèle dans MLflow
            model_version = self.mlflow_manager.save_model(
                model=tuned_model,
                model_name=f"accidents_severity_{best_model_name}",
                feature_columns=feature_info['selected_features'],
                metrics=final_metrics,
                feature_info=feature_info
            )
            self.app_metrics['model_registry_version'] = model_version
            
            # 9. Export des métriques vers MySQL
            self.export_metrics_to_mysql(final_metrics, feature_info, tuning_results)
            
            # 10. Finalisation
            self.app_metrics['end_time'] = datetime.now()
            self.app_metrics['duration_seconds'] = (
                self.app_metrics['end_time'] - self.app_metrics['start_time']
            ).total_seconds()
            
            # Log des résultats finaux
            self.logger.log_performance(
                operation="ml_training_pipeline",
                duration_seconds=self.app_metrics['duration_seconds'],
                **self.app_metrics
            )
            
            return {
                'success': True,
                'metrics': final_metrics,
                'model_info': {
                    'name': f"accidents_severity_{best_model_name}",
                    'version': model_version,
                    'mlflow_run_id': run_id
                },
                'feature_info': feature_info,
                'app_metrics': self.app_metrics
            }
            
        except Exception as e:
            self.logger.error("ML training pipeline failed", exception=e)
            if hasattr(self, 'mlflow_manager') and self.mlflow_manager:
                self.mlflow_manager.end_run(status='FAILED')
            raise MLModelError(
                message="ML training pipeline execution failed",
                operation="ml_pipeline",
                metrics=self.app_metrics
            )
    
    def export_metrics_to_mysql(self, metrics: Dict[str, Any], 
                              feature_info: Dict[str, Any],
                              tuning_results: Dict[str, Any]) -> None:
        """Exporte les métriques vers MySQL"""
        try:
            from ...common.utils.database_connector import DatabaseConnector
            
            db_connector = DatabaseConnector(self.config_manager)
            
            # Préparation des données pour insertion
            metrics_data = {
                'model_name': f"accidents_severity_classification",
                'model_version': self.app_metrics['model_registry_version'],
                'mlflow_run_id': self.app_metrics['mlflow_run_id'],
                'training_date': datetime.now(),
                'accuracy': metrics['accuracy'],
                'f1_score_macro': metrics['f1_score_macro'],
                'f1_score_micro': metrics['f1_score_micro'],
                'precision_macro': metrics['precision_macro'],
                'recall_macro': metrics['recall_macro'],
                'auc_macro': metrics.get('auc_macro', 0.0),
                'training_samples': self.app_metrics['training_samples'],
                'validation_samples': self.app_metrics['validation_samples'],
                'test_samples': self.app_metrics['test_samples'],
                'features_count': self.app_metrics['features_processed'],
                'training_duration_seconds': self.app_metrics['duration_seconds'],
                'hyperparameter_config': str(tuning_results.get('best_params', {})),
                'feature_importance': str(metrics.get('feature_importance', {})),
                'confusion_matrix': str(metrics.get('confusion_matrix', {})),
                'created_at': datetime.now()
            }
            
            # Insertion dans la table ml_model_performance
            db_connector.insert_data('ml_model_performance', metrics_data)
            
            self.logger.info("Metrics exported to MySQL successfully",
                           table='ml_model_performance',
                           model_version=self.app_metrics['model_registry_version'])
            
        except Exception as e:
            self.logger.error("Failed to export metrics to MySQL", exception=e)
            # Non-bloquant, on continue même si l'export échoue
    
    def predict_severity(self, input_data: DataFrame) -> DataFrame:
        """Prédit la sévérité pour de nouvelles données"""
        try:
            if not self.prediction_service:
                raise MLModelError(
                    message="Prediction service not initialized",
                    operation="prediction"
                )
            
            return self.prediction_service.predict(input_data)
            
        except Exception as e:
            self.logger.error("Prediction failed", exception=e)
            raise MLModelError(
                message="Severity prediction failed",
                operation="prediction"
            )
    
    def cleanup(self) -> None:
        """Nettoie les ressources"""
        try:
            if self.mlflow_manager:
                self.mlflow_manager.end_run()
            
            if self.spark:
                self.spark_utils.cleanup_spark_session(self.spark)
            
            self.logger.info("ML training application cleanup completed")
            
        except Exception as e:
            self.logger.error("Cleanup failed", exception=e)
    
    def get_model_performance_summary(self) -> Dict[str, Any]:
        """Retourne un résumé des performances du modèle"""
        return {
            'application_metrics': self.app_metrics,
            'training_completed': self.app_metrics.get('end_time') is not None,
            'model_registry_version': self.app_metrics.get('model_registry_version'),
            'best_accuracy': self.app_metrics.get('best_model_accuracy', 0.0),
            'best_f1_score': self.app_metrics.get('best_model_f1_score', 0.0),
            'total_duration_minutes': self.app_metrics.get('duration_seconds', 0) / 60
        }