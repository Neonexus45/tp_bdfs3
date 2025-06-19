"""
Tests complets pour l'application MLTRAINING
"""

import pytest
import time
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pandas as pd

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import MLModelError, DataValidationError

from .mltraining_app import MLTrainingApp
from .feature_processor import FeatureProcessor
from .model_trainer import ModelTrainer
from .model_evaluator import ModelEvaluator
from .hyperparameter_tuner import HyperparameterTuner
from .mlflow_manager import MLflowManager
from .prediction_service import PredictionService


class TestMLTrainingApp:
    """Tests pour l'application principale MLTrainingApp"""
    
    @pytest.fixture
    def config_manager(self):
        """Configuration de test"""
        config = ConfigManager()
        return config
    
    @pytest.fixture
    def spark_session(self):
        """Session Spark de test"""
        spark = SparkSession.builder \
            .appName("MLTraining-Test") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_data(self, spark_session):
        """Données d'exemple pour les tests"""
        schema = StructType([
            StructField("Severity", IntegerType(), True),
            StructField("Distance_mi", DoubleType(), True),
            StructField("Temperature_F", DoubleType(), True),
            StructField("Humidity_percent", DoubleType(), True),
            StructField("Pressure_in", DoubleType(), True),
            StructField("Visibility_mi", DoubleType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("State", StringType(), True),
            StructField("Weather_Condition", StringType(), True)
        ])
        
        data = [
            (2, 0.5, 65.0, 60.0, 29.8, 10.0, 40.7128, -74.0060, "NY", "Clear"),
            (3, 1.2, 45.0, 80.0, 30.2, 5.0, 34.0522, -118.2437, "CA", "Rain"),
            (1, 0.3, 75.0, 45.0, 29.9, 15.0, 41.8781, -87.6298, "IL", "Cloudy"),
            (4, 2.1, 35.0, 90.0, 29.5, 2.0, 29.7604, -95.3698, "TX", "Snow"),
            (2, 0.8, 68.0, 55.0, 30.0, 12.0, 39.9526, -75.1652, "PA", "Clear")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def ml_app(self, config_manager):
        """Instance de MLTrainingApp pour les tests"""
        return MLTrainingApp(config_manager)
    
    def test_ml_app_initialization(self, ml_app):
        """Test l'initialisation de l'application ML"""
        assert ml_app.config_manager is not None
        assert ml_app.logger is not None
        assert ml_app.target_column == 'Severity'
        assert ml_app.train_ratio == 0.70
        assert ml_app.validation_ratio == 0.15
        assert ml_app.test_ratio == 0.15
        assert ml_app.app_metrics['start_time'] is None
    
    @patch('src.applications.mltraining.mltraining_app.SparkUtils')
    def test_initialize_components(self, mock_spark_utils, ml_app):
        """Test l'initialisation des composants ML"""
        # Mock des composants
        mock_spark = Mock()
        mock_spark_utils.return_value.create_spark_session.return_value = mock_spark
        
        with patch.multiple(
            'src.applications.mltraining.mltraining_app',
            FeatureProcessor=Mock(),
            ModelTrainer=Mock(),
            ModelEvaluator=Mock(),
            HyperparameterTuner=Mock(),
            MLflowManager=Mock(),
            PredictionService=Mock()
        ):
            ml_app.initialize_components()
            
            assert ml_app.spark is not None
            assert ml_app.feature_processor is not None
            assert ml_app.model_trainer is not None
            assert ml_app.model_evaluator is not None
    
    def test_split_data(self, ml_app, sample_data):
        """Test la division des données"""
        train_df, val_df, test_df = ml_app.split_data(sample_data)
        
        # Vérification que les DataFrames ne sont pas vides
        assert train_df.count() > 0
        assert val_df.count() >= 0  # Peut être 0 avec peu de données
        assert test_df.count() >= 0
        
        # Vérification que le total correspond
        total_original = sample_data.count()
        total_split = train_df.count() + val_df.count() + test_df.count()
        assert total_split == total_original
    
    def test_get_model_performance_summary(self, ml_app):
        """Test le résumé des performances"""
        summary = ml_app.get_model_performance_summary()
        
        assert 'application_metrics' in summary
        assert 'training_completed' in summary
        assert 'total_duration_minutes' in summary
        assert summary['training_completed'] is False


class TestFeatureProcessor:
    """Tests pour le composant FeatureProcessor"""
    
    @pytest.fixture
    def feature_processor(self, spark_session):
        """Instance de FeatureProcessor pour les tests"""
        config = ConfigManager()
        return FeatureProcessor(config, spark_session)
    
    @pytest.fixture
    def sample_raw_data(self, spark_session):
        """Données brutes d'exemple"""
        schema = StructType([
            StructField("Severity", IntegerType(), True),
            StructField("Distance_mi", DoubleType(), True),
            StructField("Temperature_F", DoubleType(), True),
            StructField("Humidity_percent", DoubleType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("State", StringType(), True),
            StructField("Weather_Condition", StringType(), True),
            StructField("Amenity", StringType(), True),
            StructField("Traffic_Signal", StringType(), True)
        ])
        
        data = [
            (2, 0.5, 65.0, 60.0, 40.7128, -74.0060, "NY", "Clear", "False", "True"),
            (3, 1.2, 45.0, 80.0, 34.0522, -118.2437, "CA", "Rain", "True", "False"),
            (1, None, 75.0, 45.0, 41.8781, -87.6298, "IL", "Cloudy", "False", "True")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_feature_processor_initialization(self, feature_processor):
        """Test l'initialisation du FeatureProcessor"""
        assert feature_processor.target_column == 'Severity'
        assert feature_processor.max_correlation_threshold == 0.95
        assert len(feature_processor.numeric_features) > 0
        assert len(feature_processor.categorical_features) > 0
    
    def test_analyze_initial_data(self, feature_processor, sample_raw_data):
        """Test l'analyse initiale des données"""
        analysis = feature_processor._analyze_initial_data(sample_raw_data)
        
        assert 'total_rows' in analysis
        assert 'total_columns' in analysis
        assert 'column_types' in analysis
        assert 'missing_values' in analysis
        assert 'target_distribution' in analysis
        assert analysis['total_rows'] == 3
    
    def test_clean_data(self, feature_processor, sample_raw_data):
        """Test le nettoyage des données"""
        cleaned_df = feature_processor._clean_data(sample_raw_data)
        
        # Vérification que le DataFrame n'est pas vide
        assert cleaned_df.count() > 0
        
        # Vérification que les colonnes principales sont présentes
        assert 'Severity' in cleaned_df.columns
        assert 'Temperature_F' in cleaned_df.columns
    
    def test_handle_missing_values(self, feature_processor, sample_raw_data):
        """Test la gestion des valeurs manquantes"""
        imputed_df = feature_processor._handle_missing_values(sample_raw_data)
        
        # Vérification que les valeurs nulles ont été traitées
        null_count = imputed_df.filter(imputed_df.Distance_mi.isNull()).count()
        assert null_count == 0  # Les valeurs nulles doivent être imputées


class TestModelTrainer:
    """Tests pour le composant ModelTrainer"""
    
    @pytest.fixture
    def model_trainer(self, spark_session):
        """Instance de ModelTrainer pour les tests"""
        config = ConfigManager()
        return ModelTrainer(config, spark_session)
    
    @pytest.fixture
    def prepared_data(self, spark_session):
        """Données préparées pour l'entraînement"""
        from pyspark.ml.linalg import Vectors
        from pyspark.sql.types import StructType, StructField, IntegerType
        from pyspark.ml.linalg import VectorUDT
        
        schema = StructType([
            StructField("features", VectorUDT(), True),
            StructField("Severity", IntegerType(), True)
        ])
        
        data = [
            (Vectors.dense([0.5, 65.0, 60.0, 40.7, -74.0]), 2),
            (Vectors.dense([1.2, 45.0, 80.0, 34.0, -118.2]), 3),
            (Vectors.dense([0.3, 75.0, 45.0, 41.8, -87.6]), 1),
            (Vectors.dense([2.1, 35.0, 90.0, 29.7, -95.3]), 4),
            (Vectors.dense([0.8, 68.0, 55.0, 39.9, -75.1]), 2)
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_model_trainer_initialization(self, model_trainer):
        """Test l'initialisation du ModelTrainer"""
        assert model_trainer.random_seed == 42
        assert model_trainer.cv_folds == 3
        assert 'random_forest' in model_trainer.default_params
        assert 'gradient_boosting' in model_trainer.default_params
        assert 'logistic_regression' in model_trainer.default_params
    
    def test_calculate_metrics(self, model_trainer, prepared_data):
        """Test le calcul des métriques"""
        # Ajout d'une colonne prediction pour simuler des prédictions
        predictions_df = prepared_data.withColumn('prediction', prepared_data.Severity)
        
        metrics = model_trainer._calculate_metrics(predictions_df, 'Severity')
        
        assert 'accuracy' in metrics
        assert 'f1_score_macro' in metrics
        assert 'precision_macro' in metrics
        assert 'recall_macro' in metrics
        assert metrics['accuracy'] == 1.0  # Prédictions parfaites
    
    def test_create_base_model(self, model_trainer):
        """Test la création des modèles de base"""
        rf_model = model_trainer._create_base_model('random_forest', 'Severity')
        assert rf_model is not None
        
        gb_model = model_trainer._create_base_model('gradient_boosting', 'Severity')
        assert gb_model is not None
        
        lr_model = model_trainer._create_base_model('logistic_regression', 'Severity')
        assert lr_model is not None
        
        dt_model = model_trainer._create_base_model('decision_tree', 'Severity')
        assert dt_model is not None
    
    def test_get_training_summary(self, model_trainer):
        """Test le résumé d'entraînement"""
        summary = model_trainer.get_training_summary()
        
        assert 'training_metrics' in summary
        assert 'supported_models' in summary
        assert 'default_parameters' in summary
        assert len(summary['supported_models']) == 4


class TestModelEvaluator:
    """Tests pour le composant ModelEvaluator"""
    
    @pytest.fixture
    def model_evaluator(self, spark_session):
        """Instance de ModelEvaluator pour les tests"""
        config = ConfigManager()
        return ModelEvaluator(config, spark_session)
    
    @pytest.fixture
    def predictions_data(self, spark_session):
        """Données de prédictions pour les tests"""
        schema = StructType([
            StructField("Severity", IntegerType(), True),
            StructField("prediction", DoubleType(), True)
        ])
        
        data = [
            (2, 2.0),
            (3, 3.0),
            (1, 1.0),
            (4, 3.0),  # Erreur de prédiction
            (2, 2.0)
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_model_evaluator_initialization(self, model_evaluator):
        """Test l'initialisation du ModelEvaluator"""
        assert model_evaluator.severity_classes == [1, 2, 3, 4]
        assert len(model_evaluator.class_names) == 4
        assert model_evaluator.class_names[1] == 'Minor'
        assert model_evaluator.class_names[4] == 'Severe'
    
    def test_validate_predictions(self, model_evaluator, predictions_data):
        """Test la validation des prédictions"""
        # Ne doit pas lever d'exception
        model_evaluator._validate_predictions(predictions_data, 'Severity')
    
    def test_calculate_global_metrics(self, model_evaluator, predictions_data):
        """Test le calcul des métriques globales"""
        metrics = model_evaluator._calculate_global_metrics(predictions_data, 'Severity')
        
        assert 'accuracy' in metrics
        assert 'f1_score_macro' in metrics
        assert 'precision_macro' in metrics
        assert 'recall_macro' in metrics
        assert 0.0 <= metrics['accuracy'] <= 1.0
    
    def test_calculate_confusion_matrix(self, model_evaluator, predictions_data):
        """Test le calcul de la matrice de confusion"""
        confusion_matrix = model_evaluator._calculate_confusion_matrix(predictions_data, 'Severity')
        
        assert 'matrix' in confusion_matrix
        assert 'matrix_percentages' in confusion_matrix
        assert 'row_totals' in confusion_matrix
        assert 'total_samples' in confusion_matrix
        assert confusion_matrix['total_samples'] == 5
    
    def test_get_evaluation_summary(self, model_evaluator):
        """Test le résumé d'évaluation"""
        summary = model_evaluator.get_evaluation_summary()
        
        assert 'evaluation_metrics' in summary
        assert 'supported_classes' in summary
        assert 'class_names' in summary


class TestHyperparameterTuner:
    """Tests pour le composant HyperparameterTuner"""
    
    @pytest.fixture
    def hyperparameter_tuner(self, spark_session):
        """Instance de HyperparameterTuner pour les tests"""
        config = ConfigManager()
        return HyperparameterTuner(config, spark_session)
    
    def test_hyperparameter_tuner_initialization(self, hyperparameter_tuner):
        """Test l'initialisation du HyperparameterTuner"""
        assert hyperparameter_tuner.cv_folds == 3
        assert hyperparameter_tuner.random_seed == 42
        assert 'random_forest' in hyperparameter_tuner.param_grids
        assert 'gradient_boosting' in hyperparameter_tuner.param_grids
    
    def test_generate_random_params(self, hyperparameter_tuner):
        """Test la génération de paramètres aléatoires"""
        params = hyperparameter_tuner._generate_random_params('random_forest', 5)
        
        assert len(params) == 5
        assert all(isinstance(param_set, dict) for param_set in params)
        
        # Vérification que les paramètres sont dans les bonnes plages
        for param_set in params:
            if 'numTrees' in param_set:
                assert param_set['numTrees'] in hyperparameter_tuner.param_grids['random_forest']['numTrees']
    
    def test_get_default_params(self, hyperparameter_tuner):
        """Test la récupération des paramètres par défaut"""
        rf_defaults = hyperparameter_tuner._get_default_params('random_forest')
        assert 'numTrees' in rf_defaults
        assert 'maxDepth' in rf_defaults
        
        lr_defaults = hyperparameter_tuner._get_default_params('logistic_regression')
        assert 'regParam' in lr_defaults
        assert 'maxIter' in lr_defaults
    
    def test_get_tuning_summary(self, hyperparameter_tuner):
        """Test le résumé de tuning"""
        summary = hyperparameter_tuner.get_tuning_summary()
        
        assert 'tuning_metrics' in summary
        assert 'supported_models' in summary
        assert 'param_grids' in summary
        assert 'tuning_methods' in summary


class TestMLflowManager:
    """Tests pour le composant MLflowManager"""
    
    @pytest.fixture
    def mlflow_manager(self):
        """Instance de MLflowManager pour les tests"""
        config = ConfigManager()
        # Override pour les tests
        config._config['mlflow']['tracking_uri'] = 'file:///tmp/mlflow'
        config._config['mlflow']['experiment_name'] = 'test_experiment'
        
        with patch('mlflow.set_tracking_uri'), \
             patch('src.applications.mltraining.mlflow_manager.MlflowClient') as mock_client:
            
            mock_client.return_value.get_experiment_by_name.return_value = None
            mock_client.return_value.create_experiment.return_value = 'test_exp_id'
            
            return MLflowManager(config)
    
    def test_mlflow_manager_initialization(self, mlflow_manager):
        """Test l'initialisation du MLflowManager"""
        assert mlflow_manager.tracking_uri == 'file:///tmp/mlflow'
        assert mlflow_manager.experiment_name == 'test_experiment'
        assert mlflow_manager.client is not None
    
    @patch('mlflow.start_run')
    def test_start_experiment_run(self, mock_start_run, mlflow_manager):
        """Test le démarrage d'un run d'expérience"""
        mock_run = Mock()
        mock_run.info.run_id = 'test_run_id'
        mock_start_run.return_value = mock_run
        
        run_id = mlflow_manager.start_experiment_run(run_name='test_run')
        
        assert run_id == 'test_run_id'
        assert mlflow_manager.current_run_id == 'test_run_id'
    
    def test_get_manager_summary(self, mlflow_manager):
        """Test le résumé du gestionnaire"""
        summary = mlflow_manager.get_manager_summary()
        
        assert 'tracking_uri' in summary
        assert 'experiment_name' in summary
        assert 'manager_metrics' in summary


class TestPredictionService:
    """Tests pour le composant PredictionService"""
    
    @pytest.fixture
    def prediction_service(self, spark_session):
        """Instance de PredictionService pour les tests"""
        config = ConfigManager()
        return PredictionService(config, spark_session)
    
    @pytest.fixture
    def input_data(self, spark_session):
        """Données d'entrée pour les prédictions"""
        schema = StructType([
            StructField("Distance_mi", DoubleType(), True),
            StructField("Temperature_F", DoubleType(), True),
            StructField("Humidity_percent", DoubleType(), True),
            StructField("Pressure_in", DoubleType(), True),
            StructField("Visibility_mi", DoubleType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True)
        ])
        
        data = [
            (0.5, 65.0, 60.0, 29.8, 10.0, 40.7128, -74.0060),
            (1.2, 45.0, 80.0, 30.2, 5.0, 34.0522, -118.2437)
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_prediction_service_initialization(self, prediction_service):
        """Test l'initialisation du PredictionService"""
        assert prediction_service.severity_classes == [1, 2, 3, 4]
        assert len(prediction_service.class_names) == 4
        assert prediction_service.confidence_threshold == 0.5
        assert prediction_service.loaded_model is None
    
    def test_validate_input_data(self, prediction_service, input_data):
        """Test la validation des données d'entrée"""
        # Ne doit pas lever d'exception
        prediction_service._validate_input_data(input_data)
    
    def test_dict_to_dataframe(self, prediction_service):
        """Test la conversion dictionnaire vers DataFrame"""
        record = {
            'Distance_mi': 0.5,
            'Temperature_F': 65.0,
            'Humidity_percent': 60.0,
            'Start_Lat': 40.7128,
            'Start_Lng': -74.0060
        }
        
        df = prediction_service._dict_to_dataframe(record)
        
        assert df.count() == 1
        assert len(df.columns) == 5
        assert df.collect()[0]['Distance_mi'] == 0.5
    
    def test_get_model_info(self, prediction_service):
        """Test les informations du modèle"""
        info = prediction_service.get_model_info()
        
        assert 'model_name' in info
        assert 'is_loaded' in info
        assert 'feature_columns' in info
        assert info['is_loaded'] is False
    
    def test_get_service_metrics(self, prediction_service):
        """Test les métriques du service"""
        metrics = prediction_service.get_service_metrics()
        
        assert 'predictions_made' in metrics
        assert 'batch_predictions' in metrics
        assert 'single_predictions' in metrics
        assert 'avg_prediction_time' in metrics
    
    def test_health_check(self, prediction_service):
        """Test le health check"""
        health = prediction_service.health_check()
        
        assert 'status' in health
        assert 'model_loaded' in health
        assert 'timestamp' in health
        assert health['status'] in ['healthy', 'unhealthy']


class TestIntegration:
    """Tests d'intégration pour l'application complète"""
    
    @pytest.fixture
    def spark_session(self):
        """Session Spark pour les tests d'intégration"""
        spark = SparkSession.builder \
            .appName("MLTraining-Integration-Test") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def integration_data(self, spark_session):
        """Données d'intégration plus complètes"""
        schema = StructType([
            StructField("Severity", IntegerType(), True),
            StructField("Distance_mi", DoubleType(), True),
            StructField("Temperature_F", DoubleType(), True),
            StructField("Humidity_percent", DoubleType(), True),
            StructField("Pressure_in", DoubleType(), True),
            StructField("Visibility_mi", DoubleType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("State", StringType(), True),
            StructField("Weather_Condition", StringType(), True),
            StructField("accident_hour", IntegerType(), True),
            StructField("accident_day_of_week", IntegerType(), True),
            StructField("is_weekend", StringType(), True),
            StructField("is_rush_hour", StringType(), True)
        ])
        
        # Génération de données plus réalistes
        data = []
        for i in range(100):
            severity = (i % 4) + 1
            data.append((
                severity,
                0.1 + (i % 10) * 0.2,  # Distance
                30.0 + (i % 50),       # Temperature
                20.0 + (i % 60),       # Humidity
                29.0 + (i % 3),        # Pressure
                1.0 + (i % 15),        # Visibility
                30.0 + (i % 20),       # Lat
                -120.0 + (i % 40),     # Lng
                ["CA", "NY", "TX", "FL"][i % 4],  # State
                ["Clear", "Rain", "Snow", "Cloudy"][i % 4],  # Weather
                i % 24,                # Hour
                (i % 7) + 1,          # Day of week
                "True" if (i % 7) in [0, 6] else "False",  # Weekend
                "True" if (i % 24) in [7, 8, 17, 18] else "False"  # Rush hour
            ))
        
        return spark_session.createDataFrame(data, schema)
    
    def test_feature_processor_integration(self, integration_data):
        """Test d'intégration du FeatureProcessor"""
        config = ConfigManager()
        feature_processor = FeatureProcessor(config, integration_data.sql_ctx.sparkSession)
        
        # Test du preprocessing complet
        processed_df, feature_info = feature_processor.prepare_features_for_ml(integration_data)
        
        assert processed_df.count() > 0
        assert 'selected_features' in feature_info
        assert 'feature_vector_column' in feature_info
        assert len(feature_info['selected_features']) > 0
        assert feature_info['feature_vector_column'] == 'features'
    
    @patch('src.applications.mltraining.mltraining_app.MLflowManager')
    def test_ml_pipeline_integration(self, mock_mlflow_manager, integration_data):
        """Test d'intégration du pipeline ML complet"""
        # Configuration des mocks
        mock_mlflow = Mock()
        mock_mlflow.start_experiment_run.return_value = 'test_run_id'
        mock_mlflow.save_model.return_value = 'v1'
        mock_mlflow_manager.return_value = mock_mlflow
        
        config = ConfigManager()
        ml_app = MLTrainingApp(config)
        
        # Mock de la session Spark
        ml_app.spark = integration_data.sql_ctx.sparkSession
        
        # Mock des composants pour éviter les erreurs MLflow
        with patch.multiple(
            ml_app,
            feature_processor=Mock(),
            model_trainer=Mock(),
            model_evaluator=Mock(),
            hyperparameter_tuner=Mock(),
            prediction_service=Mock()
        ):
            # Configuration des mocks
            ml_app.feature_processor.prepare_features_for_ml.return_value = (
                integration_data, 
                {'selected_features': ['Distance_mi', 'Temperature_F'], 'feature_vector_column': 'features'}
            )
            
            ml_app.model_trainer.train_multiple_models.return_value = {
                'random_forest': {
                    'validation_metrics': {'f1_score_macro': 0.85, 'accuracy': 0.80},
                    'model': Mock()
                }
            }
            
            ml_app.hyperparameter_tuner.tune_model.return_value = (
                Mock(), 
                {'best_score': 0.87, 'best_params': {'numTrees': 100}}
            )
            
            ml_app.model_evaluator.evaluate_model.return_value = {
                'accuracy': 0.85,
                'f1_score_macro': 0.87,
                'confusion_matrix': {},
                'feature_importance': {}
            }
            
            # Test du pipeline complet
            with patch.object(ml_app, 'load_training_data', return_value=integration_data):
                with patch.object(ml_app, 'export_metrics_to_mysql'):
                    result = ml_app.run_ml_pipeline()
            
            assert result['success'] is True
            assert 'metrics' in result
            assert 'model_info' in result
            assert 'feature_info' in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])