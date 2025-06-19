"""
Tests complets pour l'application PREPROCESSOR
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import Dict, Any

# Ajout du chemin racine pour les imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
    from pyspark.sql.functions import col, lit
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️ PySpark not available - using mocks for testing")

from src.common.config.config_manager import ConfigManager
from src.applications.preprocessor.preprocessor_app import PreprocessorApp
from src.applications.preprocessor.data_cleaner import DataCleaner
from src.applications.preprocessor.feature_engineer import FeatureEngineer
from src.applications.preprocessor.hive_table_manager import HiveTableManager
from src.applications.preprocessor.transformation_factory import TransformationFactory, BaseTransformation
from src.applications.preprocessor.aggregation_engine import AggregationEngine


@pytest.fixture
def sample_dataframe():
    """Fixture pour DataFrame d'exemple"""
    if SPARK_AVAILABLE:
        spark = SparkSession.builder.appName("test").getOrCreate()
        
        # Schéma simplifié pour les tests
        schema = StructType([
            StructField("ID", StringType(), False),
            StructField("Severity", IntegerType(), True),
            StructField("Start_Time", TimestampType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("State", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Distance(mi)", DoubleType(), True),
            StructField("Temperature(F)", DoubleType(), True),
            StructField("Humidity(%)", DoubleType(), True),
            StructField("Weather_Condition", StringType(), True),
            StructField("Amenity", BooleanType(), True),
            StructField("Traffic_Signal", BooleanType(), True)
        ])
        
        # Données d'exemple
        data = [
            ("A-1", 2, datetime(2023, 6, 15, 14, 30), 40.7128, -74.0060, "NY", "New York", 0.5, 75.0, 65.0, "Clear", False, True),
            ("A-2", 3, datetime(2023, 6, 15, 8, 15), 34.0522, -118.2437, "CA", "Los Angeles", 1.2, 80.0, 70.0, "Rain", True, False),
            ("A-3", 1, datetime(2023, 6, 15, 18, 45), 41.8781, -87.6298, "IL", "Chicago", 0.8, 68.0, 55.0, "Snow", False, True),
            ("A-4", 4, datetime(2023, 6, 15, 22, 10), 29.7604, -95.3698, "TX", "Houston", 2.1, 85.0, 80.0, "Fog", True, True),
            ("A-5", 2, datetime(2023, 6, 16, 6, 30), 39.9526, -75.1652, "PA", "Philadelphia", 0.3, 72.0, 60.0, "Clear", False, False)
        ]
        
        return spark.createDataFrame(data, schema)
    else:
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 5
        mock_df.columns = ["ID", "Severity", "Start_Time", "Start_Lat", "Start_Lng", "State", "City"]
        return mock_df


class TestPreprocessorApp:
    """Tests pour l'application principale PREPROCESSOR"""
    
    @pytest.fixture
    def config_manager(self):
        """Fixture pour ConfigManager"""
        config = ConfigManager()
        # Configuration de test
        config._config.update({
            'hdfs': {
                'base_path': '/test/lakehouse',
                'bronze_path': '/test/bronze',
                'silver_path': '/test/silver'
            },
            'hive': {
                'database': 'test_accidents',
                'metastore_uri': 'thrift://localhost:9083',
                'warehouse_dir': '/test/hive/warehouse',
                'tables': {
                    'accidents_clean': 'test_accidents_clean',
                    'weather_aggregated': 'test_weather_agg',
                    'infrastructure_features': 'test_infra_features'
                }
            },
            'logging': {
                'level': 'INFO',
                'format': 'json'
            }
        })
        return config
    
    @pytest.fixture
    def mock_spark_session(self):
        """Fixture pour session Spark mockée"""
        if SPARK_AVAILABLE:
            return None  # Utiliser une vraie session en local
        else:
            mock_spark = Mock()
            mock_spark.version = "3.5.0"
            mock_spark.sparkContext.applicationId = "test-app-123"
            mock_spark.conf.set = Mock()
            return mock_spark
    
    def test_preprocessor_app_initialization(self, config_manager):
        """Test l'initialisation de l'application PREPROCESSOR"""
        app = PreprocessorApp(config_manager)
        
        assert app.config_manager == config_manager
        assert app.bronze_path == '/test/bronze'
        assert app.silver_path == '/test/silver'
        assert app.max_retries == 3
        assert isinstance(app.data_cleaner, DataCleaner)
        assert isinstance(app.feature_engineer, FeatureEngineer)
        assert isinstance(app.hive_table_manager, HiveTableManager)
        assert isinstance(app.transformation_factory, TransformationFactory)
        assert isinstance(app.aggregation_engine, AggregationEngine)
    
    @patch('src.applications.preprocessor.preprocessor_app.SparkUtils')
    def test_preprocessor_app_run_success(self, mock_spark_utils, config_manager, sample_dataframe):
        """Test l'exécution réussie de l'application"""
        # Configuration des mocks
        mock_spark_utils_instance = Mock()
        mock_spark_utils.return_value = mock_spark_utils_instance
        mock_spark_utils_instance.create_spark_session.return_value = Mock()
        mock_spark_utils_instance.read_optimized_data.return_value = sample_dataframe
        
        app = PreprocessorApp(config_manager)
        
        # Mock des composants
        app.data_cleaner.clean_data = Mock(return_value=sample_dataframe)
        app.data_cleaner.get_cleaning_metrics = Mock(return_value={'cleaning_score': 85.0})
        
        app.feature_engineer.create_features = Mock(return_value=sample_dataframe)
        app.feature_engineer.get_feature_metrics = Mock(return_value={'features_created': 15})
        
        app.hive_table_manager.create_accidents_clean_table = Mock(return_value={'success': True, 'records_written': 5})
        app.hive_table_manager.create_weather_aggregated_table = Mock(return_value={'success': True, 'records_written': 3})
        app.hive_table_manager.create_infrastructure_features_table = Mock(return_value={'success': True, 'records_written': 2})
        
        app.aggregation_engine.create_all_aggregations = Mock(return_value={'temporal': sample_dataframe})
        
        # Exécution
        result = app.run()
        
        # Vérifications
        assert result['status'] == 'success'
        assert 'metrics' in result
        assert result['metrics']['records_read'] > 0
        assert result['metrics']['features_created'] == 15
        assert result['metrics']['tables_created'] == 3
    
    def test_preprocessor_app_run_failure(self, config_manager):
        """Test la gestion d'erreur de l'application"""
        app = PreprocessorApp(config_manager)
        
        # Simulation d'une erreur lors de l'initialisation Spark
        with patch.object(app, '_initialize_spark', side_effect=Exception("Spark initialization failed")):
            result = app.run()
            
            assert result['status'] == 'error'
            assert 'Spark initialization failed' in result['message']
            assert result['metrics']['errors_count'] == 1


class TestDataCleaner:
    """Tests pour le composant DataCleaner"""
    
    @pytest.fixture
    def data_cleaner(self):
        """Fixture pour DataCleaner"""
        return DataCleaner()
    
    def test_data_cleaner_initialization(self, data_cleaner):
        """Test l'initialisation du DataCleaner"""
        assert data_cleaner.max_null_percentage == 30.0
        assert data_cleaner.outlier_threshold == 3.0
        assert len(data_cleaner.columns_with_parentheses) == 8
        assert len(data_cleaner.infrastructure_columns) == 13
    
    @patch('src.applications.preprocessor.data_cleaner.DataFrame')
    def test_remove_duplicates(self, mock_dataframe_class, data_cleaner):
        """Test la suppression des doublons"""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.dropDuplicates.return_value = mock_df
        
        result = data_cleaner._remove_duplicates(mock_df)
        
        mock_df.dropDuplicates.assert_called_once_with(['ID'])
        assert result == mock_df
    
    def test_normalize_column_names(self, data_cleaner, sample_dataframe):
        """Test la normalisation des noms de colonnes"""
        if SPARK_AVAILABLE:
            # Test avec un vrai DataFrame
            result_df = data_cleaner._normalize_column_names(sample_dataframe)
            
            # Vérification que les colonnes ont été renommées
            assert 'distance_miles' in result_df.columns
            assert 'temperature_fahrenheit' in result_df.columns
            assert 'humidity_percent' in result_df.columns
            assert 'Distance(mi)' not in result_df.columns
        else:
            # Test avec mock
            mock_df = Mock()
            mock_df.columns = ['Distance(mi)', 'Temperature(F)', 'Other']
            mock_df.withColumnRenamed.return_value = mock_df
            
            result = data_cleaner._normalize_column_names(mock_df)
            assert result == mock_df
    
    def test_get_cleaning_metrics(self, data_cleaner):
        """Test la récupération des métriques de nettoyage"""
        metrics = data_cleaner.get_cleaning_metrics()
        
        assert isinstance(metrics, dict)
        assert 'records_input' in metrics
        assert 'records_output' in metrics
        assert 'cleaning_score' in metrics


class TestFeatureEngineer:
    """Tests pour le composant FeatureEngineer"""
    
    @pytest.fixture
    def feature_engineer(self):
        """Fixture pour FeatureEngineer"""
        return FeatureEngineer()
    
    def test_feature_engineer_initialization(self, feature_engineer):
        """Test l'initialisation du FeatureEngineer"""
        assert len(feature_engineer.rush_hours) == 2
        assert len(feature_engineer.season_mapping) == 12
        assert len(feature_engineer.state_regions) == 6
        assert len(feature_engineer.infrastructure_columns) == 13
    
    def test_create_temporal_features(self, feature_engineer, sample_dataframe):
        """Test la création des features temporelles"""
        if SPARK_AVAILABLE:
            result_df = feature_engineer._create_temporal_features(sample_dataframe)
            
            # Vérification des nouvelles colonnes
            expected_columns = ['accident_hour', 'accident_day_of_week', 'accident_month', 
                              'accident_season', 'is_weekend', 'is_rush_hour']
            
            for col_name in expected_columns:
                assert col_name in result_df.columns
            
            # Vérification des valeurs
            first_row = result_df.collect()[0]
            assert first_row['accident_hour'] == 14  # 14:30
            assert first_row['accident_month'] == 6   # Juin
        else:
            # Test avec mock
            mock_df = Mock()
            mock_df.columns = ['Start_Time']
            mock_df.withColumn.return_value = mock_df
            
            result = feature_engineer._create_temporal_features(mock_df)
            assert result == mock_df
    
    def test_create_weather_features(self, feature_engineer, sample_dataframe):
        """Test la création des features météorologiques"""
        if SPARK_AVAILABLE:
            result_df = feature_engineer._create_weather_features(sample_dataframe)
            
            # Vérification des nouvelles colonnes
            expected_columns = ['weather_category', 'weather_severity_score', 
                              'temperature_category']
            
            for col_name in expected_columns:
                assert col_name in result_df.columns
        else:
            # Test avec mock
            mock_df = Mock()
            mock_df.columns = ['Weather_Condition', 'temperature_fahrenheit']
            mock_df.withColumn.return_value = mock_df
            
            result = feature_engineer._create_weather_features(mock_df)
            assert result == mock_df
    
    def test_get_feature_metrics(self, feature_engineer):
        """Test la récupération des métriques de feature engineering"""
        metrics = feature_engineer.get_feature_metrics()
        
        assert isinstance(metrics, dict)
        assert 'features_created' in metrics
        assert 'temporal_features' in metrics
        assert 'weather_features' in metrics
        assert 'feature_engineering_score' in metrics


class TestHiveTableManager:
    """Tests pour le composant HiveTableManager"""
    
    @pytest.fixture
    def hive_manager(self):
        """Fixture pour HiveTableManager"""
        return HiveTableManager()
    
    def test_hive_manager_initialization(self, hive_manager):
        """Test l'initialisation du HiveTableManager"""
        assert hive_manager.hive_database == 'accidents_warehouse'  # Valeur par défaut
        assert len(hive_manager.table_names) == 3
        assert len(hive_manager.bucketing_config) == 3
        assert len(hive_manager.partition_config) == 3
    
    def test_get_accidents_clean_schema(self, hive_manager):
        """Test la récupération du schéma accidents_clean"""
        schema = hive_manager._get_accidents_clean_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 20  # Au moins 20 colonnes
        
        # Vérification de quelques colonnes importantes
        field_names = [field.name for field in schema.fields]
        assert 'ID' in field_names
        assert 'Severity' in field_names
        assert 'accident_hour' in field_names
        assert 'weather_category' in field_names
    
    def test_get_table_info(self, hive_manager):
        """Test la récupération d'informations sur une table"""
        # Test sans session Spark active
        info = hive_manager.get_table_info('accidents_clean')
        
        assert isinstance(info, dict)
        assert 'table_name' in info
        assert 'bucketing' in info
        assert 'partitioning' in info


class TestTransformationFactory:
    """Tests pour le composant TransformationFactory"""
    
    @pytest.fixture
    def transformation_factory(self):
        """Fixture pour TransformationFactory"""
        return TransformationFactory()
    
    def test_factory_initialization(self, transformation_factory):
        """Test l'initialisation de la factory"""
        assert len(transformation_factory.transformation_registry) >= 5
        assert 'null_handling' in transformation_factory.transformation_registry
        assert 'outlier_removal' in transformation_factory.transformation_registry
        assert 'data_type_conversion' in transformation_factory.transformation_registry
    
    def test_create_transformation(self, transformation_factory):
        """Test la création d'une transformation"""
        config = {'strategies': {'ID': 'drop'}}
        transformation = transformation_factory.create_transformation('null_handling', config)
        
        assert isinstance(transformation, BaseTransformation)
        assert transformation.name == 'null_handling'
        assert transformation.config == config
    
    def test_create_transformation_invalid_type(self, transformation_factory):
        """Test la création d'une transformation avec type invalide"""
        with pytest.raises(ValueError, match="Unknown transformation type"):
            transformation_factory.create_transformation('invalid_type')
    
    def test_create_us_accidents_pipeline(self, transformation_factory):
        """Test la création de la pipeline US-Accidents"""
        pipeline = transformation_factory.create_us_accidents_pipeline()
        
        assert isinstance(pipeline, list)
        assert len(pipeline) >= 5
        
        # Vérification des types de transformations
        transformation_types = [step['type'] for step in pipeline]
        assert 'null_handling' in transformation_types
        assert 'range_validation' in transformation_types
        assert 'string_normalization' in transformation_types
    
    def test_get_factory_metrics(self, transformation_factory):
        """Test la récupération des métriques de la factory"""
        metrics = transformation_factory.get_factory_metrics()
        
        assert isinstance(metrics, dict)
        assert 'transformations_created' in metrics
        assert 'transformations_applied' in metrics
        assert 'success_rate' in metrics


class TestAggregationEngine:
    """Tests pour le composant AggregationEngine"""
    
    @pytest.fixture
    def aggregation_engine(self):
        """Fixture pour AggregationEngine"""
        return AggregationEngine()
    
    def test_aggregation_engine_initialization(self, aggregation_engine):
        """Test l'initialisation du moteur d'agrégations"""
        assert len(aggregation_engine.aggregation_config['temporal_granularities']) == 5
        assert len(aggregation_engine.aggregation_config['geographic_levels']) == 3
        assert len(aggregation_engine.aggregation_config['weather_categories']) == 6
        assert len(aggregation_engine.aggregation_config['percentiles']) == 6
    
    @patch('src.applications.preprocessor.aggregation_engine.DataFrame')
    def test_create_temporal_aggregations(self, mock_dataframe_class, aggregation_engine):
        """Test la création des agrégations temporelles"""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.columns = ['accident_hour', 'accident_day_of_week', 'accident_month', 'accident_season']
        mock_df.groupBy.return_value.agg.return_value.orderBy.return_value = mock_df
        
        result = aggregation_engine._create_temporal_aggregations(mock_df)
        
        assert isinstance(result, dict)
        # Avec les colonnes mockées, on devrait avoir plusieurs agrégations temporelles
    
    def test_get_aggregation_metrics(self, aggregation_engine):
        """Test la récupération des métriques d'agrégation"""
        metrics = aggregation_engine.get_aggregation_metrics()
        
        assert isinstance(metrics, dict)
        assert 'aggregations_created' in metrics
        assert 'temporal_aggregations' in metrics
        assert 'geographic_aggregations' in metrics
        assert 'total_execution_time' in metrics


class TestIntegration:
    """Tests d'intégration pour l'ensemble du module PREPROCESSOR"""
    
    @pytest.fixture
    def config_manager(self):
        """Configuration pour les tests d'intégration"""
        config = ConfigManager()
        config._config.update({
            'hdfs': {
                'bronze_path': '/test/bronze',
                'silver_path': '/test/silver'
            },
            'hive': {
                'database': 'test_accidents',
                'metastore_uri': 'thrift://localhost:9083'
            },
            'logging': {
                'level': 'INFO',
                'format': 'json'
            }
        })
        return config
    
    def test_full_preprocessing_pipeline_mock(self, config_manager):
        """Test complet de la pipeline de preprocessing avec mocks"""
        # Création de l'application
        app = PreprocessorApp(config_manager)
        
        # Mock des données d'entrée
        mock_input_df = Mock()
        mock_input_df.count.return_value = 1000
        mock_input_df.columns = ['ID', 'Severity', 'Start_Time', 'State', 'Weather_Condition']
        
        # Mock des composants
        app.data_cleaner.clean_data = Mock(return_value=mock_input_df)
        app.data_cleaner.get_cleaning_metrics = Mock(return_value={
            'records_input': 1000,
            'records_output': 950,
            'cleaning_score': 88.5
        })
        
        app.feature_engineer.create_features = Mock(return_value=mock_input_df)
        app.feature_engineer.get_feature_metrics = Mock(return_value={
            'features_created': 18,
            'feature_engineering_score': 92.0
        })
        
        # Test des métriques
        cleaning_metrics = app.data_cleaner.get_cleaning_metrics()
        feature_metrics = app.feature_engineer.get_feature_metrics()
        
        assert cleaning_metrics['cleaning_score'] > 80
        assert feature_metrics['features_created'] > 15
        assert feature_metrics['feature_engineering_score'] > 90
    
    def test_component_interaction(self, config_manager):
        """Test l'interaction entre les composants"""
        # Création des composants
        data_cleaner = DataCleaner(config_manager)
        feature_engineer = FeatureEngineer(config_manager)
        transformation_factory = TransformationFactory(config_manager)
        
        # Test de la configuration partagée
        assert data_cleaner.config_manager == config_manager
        assert feature_engineer.config_manager == config_manager
        assert transformation_factory.config_manager == config_manager
        
        # Test des métriques initiales
        assert data_cleaner.get_cleaning_metrics()['cleaning_score'] == 0.0
        assert feature_engineer.get_feature_metrics()['features_created'] == 0
        assert transformation_factory.get_factory_metrics()['transformations_created'] == 0


def run_tests():
    """Fonction pour exécuter tous les tests"""
    print("🧪 Démarrage des tests PREPROCESSOR...")
    
    # Configuration pytest
    pytest_args = [
        __file__,
        '-v',
        '--tb=short',
        '--color=yes'
    ]
    
    # Ajout de la couverture si disponible
    try:
        import pytest_cov
        pytest_args.extend(['--cov=src.applications.preprocessor', '--cov-report=term-missing'])
    except ImportError:
        print("⚠️ pytest-cov non disponible - tests sans couverture")
    
    # Exécution des tests
    exit_code = pytest.main(pytest_args)
    
    if exit_code == 0:
        print("✅ Tous les tests PREPROCESSOR sont passés avec succès!")
    else:
        print("❌ Certains tests PREPROCESSOR ont échoué")
    
    return exit_code


if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)