"""
Tests complets pour l'application DATAMART
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import pandas as pd

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.utils.database_connector import DatabaseConnector
from ...common.exceptions.custom_exceptions import LakehouseException, SparkJobError, DataValidationError

from .datamart_app import DatamartApp
from .business_aggregator import BusinessAggregator
from .kpi_calculator import KPICalculator
from .mysql_exporter import MySQLExporter
from .table_optimizer import TableOptimizer
from .analytics_engine import AnalyticsEngine


class TestDatamartApp:
    """Tests pour DatamartApp"""
    
    @pytest.fixture
    def mock_config_manager(self):
        """Mock ConfigManager"""
        config = Mock(spec=ConfigManager)
        config.get_hdfs_path.return_value = "/test/path"
        config.get.return_value = "test_database"
        config.get_hive_table_name.return_value = "test_table"
        return config
    
    @pytest.fixture
    def mock_logger(self):
        """Mock Logger"""
        return Mock(spec=Logger)
    
    @pytest.fixture
    def mock_spark_utils(self):
        """Mock SparkUtils"""
        spark_utils = Mock()
        spark_utils.get_spark_session.return_value = Mock(spec=SparkSession)
        return spark_utils
    
    @pytest.fixture
    def mock_db_connector(self):
        """Mock DatabaseConnector"""
        return Mock(spec=DatabaseConnector)
    
    @pytest.fixture
    def datamart_app(self, mock_config_manager):
        """Instance DatamartApp pour tests"""
        with patch('src.applications.datamart.datamart_app.SparkUtils'), \
             patch('src.applications.datamart.datamart_app.DatabaseConnector'), \
             patch('src.applications.datamart.datamart_app.Logger'):
            return DatamartApp(mock_config_manager)
    
    def test_datamart_app_initialization(self, datamart_app, mock_config_manager):
        """Test l'initialisation de DatamartApp"""
        assert datamart_app.config_manager == mock_config_manager
        assert datamart_app.max_retries == 3
        assert datamart_app.retry_delay == 5
        assert datamart_app.processing_metrics == {}
    
    @patch('src.applications.datamart.datamart_app.BusinessAggregator')
    @patch('src.applications.datamart.datamart_app.KPICalculator')
    @patch('src.applications.datamart.datamart_app.MySQLExporter')
    @patch('src.applications.datamart.datamart_app.TableOptimizer')
    @patch('src.applications.datamart.datamart_app.AnalyticsEngine')
    def test_initialize_components(self, mock_analytics, mock_optimizer, mock_exporter, 
                                 mock_kpi, mock_aggregator, datamart_app):
        """Test l'initialisation des composants"""
        datamart_app._initialize_components()
        
        assert datamart_app.business_aggregator is not None
        assert datamart_app.kpi_calculator is not None
        assert datamart_app.mysql_exporter is not None
        assert datamart_app.table_optimizer is not None
        assert datamart_app.analytics_engine is not None
    
    def test_validate_silver_data_success(self, datamart_app):
        """Test la validation des données Silver avec succès"""
        # Mock Spark session et table
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.columns = ['col1', 'col2', 'col3']
        mock_spark.table.return_value = mock_df
        
        datamart_app.spark_utils.get_spark_session.return_value = mock_spark
        
        # Test validation
        datamart_app._validate_silver_data()
        
        # Vérifications
        assert 'silver_validation' in datamart_app.processing_metrics
        validation_results = datamart_app.processing_metrics['silver_validation']
        assert len(validation_results) == 3  # 3 tables requises
    
    def test_validate_silver_data_empty_table(self, datamart_app):
        """Test la validation avec table vide"""
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 0  # Table vide
        mock_spark.table.return_value = mock_df
        
        datamart_app.spark_utils.get_spark_session.return_value = mock_spark
        
        # Test validation - doit lever une exception
        with pytest.raises(DataValidationError):
            datamart_app._validate_silver_data()
    
    def test_run_pipeline_success(self, datamart_app):
        """Test l'exécution complète du pipeline avec succès"""
        # Mock de toutes les méthodes
        datamart_app._initialize_components = Mock()
        datamart_app._validate_silver_data = Mock()
        datamart_app._perform_business_aggregations = Mock(return_value={'test': 'data'})
        datamart_app._calculate_kpis = Mock(return_value={'test': 'kpis'})
        datamart_app._export_to_mysql = Mock(return_value={'rows_exported': 1000})
        datamart_app._optimize_mysql_tables = Mock()
        datamart_app._generate_final_metrics = Mock(return_value={'status': 'SUCCESS'})
        datamart_app._cleanup_resources = Mock()
        
        # Exécution
        result = datamart_app.run()
        
        # Vérifications
        assert result['status'] == 'SUCCESS'
        datamart_app._initialize_components.assert_called_once()
        datamart_app._validate_silver_data.assert_called_once()
        datamart_app._perform_business_aggregations.assert_called_once()
        datamart_app._calculate_kpis.assert_called_once()
        datamart_app._export_to_mysql.assert_called_once()
        datamart_app._optimize_mysql_tables.assert_called_once()
        datamart_app._cleanup_resources.assert_called_once()
    
    def test_run_pipeline_failure(self, datamart_app):
        """Test l'échec du pipeline"""
        datamart_app._initialize_components = Mock()
        datamart_app._validate_silver_data = Mock(side_effect=Exception("Test error"))
        datamart_app._cleanup_resources = Mock()
        
        # Test échec
        with pytest.raises(LakehouseException):
            datamart_app.run()
        
        # Vérification du cleanup
        datamart_app._cleanup_resources.assert_called_once()
    
    def test_get_pipeline_status(self, datamart_app):
        """Test le statut du pipeline"""
        status = datamart_app.get_pipeline_status()
        
        assert 'status' in status
        assert 'start_time' in status
        assert 'current_metrics' in status
        assert 'components_initialized' in status
        assert status['status'] == 'IDLE'
    
    def test_run_incremental_update(self, datamart_app):
        """Test la mise à jour incrémentale"""
        # Mock des composants
        datamart_app.business_aggregator = Mock()
        datamart_app.kpi_calculator = Mock()
        datamart_app.mysql_exporter = Mock()
        
        datamart_app.business_aggregator.incremental_aggregation.return_value = {'test': 'data'}
        datamart_app.kpi_calculator.incremental_kpi_update.return_value = {'test': 'kpis'}
        datamart_app.mysql_exporter.incremental_export.return_value = {'rows_exported': 100}
        
        # Test
        result = datamart_app.run_incremental_update("2024-01-01T00:00:00")
        
        # Vérifications
        assert 'rows_exported' in result
        datamart_app.business_aggregator.incremental_aggregation.assert_called_once()
        datamart_app.kpi_calculator.incremental_kpi_update.assert_called_once()
        datamart_app.mysql_exporter.incremental_export.assert_called_once()


class TestBusinessAggregator:
    """Tests pour BusinessAggregator"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession"""
        spark = Mock(spec=SparkSession)
        return spark
    
    @pytest.fixture
    def mock_config_manager(self):
        """Mock ConfigManager"""
        config = Mock(spec=ConfigManager)
        config.get.return_value = "test_database"
        config.get_hive_table_name.return_value = "test_table"
        return config
    
    @pytest.fixture
    def mock_logger(self):
        """Mock Logger"""
        return Mock(spec=Logger)
    
    @pytest.fixture
    def business_aggregator(self, mock_spark, mock_config_manager, mock_logger):
        """Instance BusinessAggregator pour tests"""
        return BusinessAggregator(mock_spark, mock_config_manager, mock_logger)
    
    @pytest.fixture
    def sample_accidents_df(self, mock_spark):
        """DataFrame d'accidents simulé"""
        mock_df = Mock(spec=DataFrame)
        mock_df.select.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.union.return_value = mock_df
        mock_df.cache.return_value = mock_df
        mock_df.count.return_value = 1000
        return mock_df
    
    def test_business_aggregator_initialization(self, business_aggregator, mock_config_manager):
        """Test l'initialisation de BusinessAggregator"""
        assert business_aggregator.config_manager == mock_config_manager
        assert business_aggregator.hive_database == "test_database"
        assert len(business_aggregator.table_names) == 3
        assert business_aggregator._cached_dfs == {}
    
    def test_get_cached_accidents_data(self, business_aggregator, sample_accidents_df):
        """Test le cache des données accidents"""
        business_aggregator.spark.table.return_value = sample_accidents_df
        
        # Premier appel - doit charger et cacher
        df1 = business_aggregator._get_cached_accidents_data()
        assert df1 == sample_accidents_df
        assert 'accidents' in business_aggregator._cached_dfs
        
        # Deuxième appel - doit utiliser le cache
        df2 = business_aggregator._get_cached_accidents_data()
        assert df2 == sample_accidents_df
        business_aggregator.spark.table.assert_called_once()  # Appelé une seule fois
    
    def test_aggregate_temporal_patterns(self, business_aggregator, sample_accidents_df):
        """Test l'agrégation des patterns temporels"""
        business_aggregator._get_cached_accidents_data = Mock(return_value=sample_accidents_df)
        
        result = business_aggregator.aggregate_temporal_patterns()
        
        assert result is not None
        business_aggregator._get_cached_accidents_data.assert_called_once()
    
    def test_aggregate_geographic_patterns(self, business_aggregator, sample_accidents_df):
        """Test l'agrégation des patterns géographiques"""
        business_aggregator._get_cached_accidents_data = Mock(return_value=sample_accidents_df)
        
        result = business_aggregator.aggregate_geographic_patterns()
        
        assert result is not None
        business_aggregator._get_cached_accidents_data.assert_called_once()
    
    def test_clear_cache(self, business_aggregator):
        """Test le nettoyage du cache"""
        # Ajout d'un DataFrame mocké au cache
        mock_df = Mock()
        business_aggregator._cached_dfs['test'] = mock_df
        
        # Nettoyage
        business_aggregator._clear_cache()
        
        # Vérifications
        mock_df.unpersist.assert_called_once()
        assert business_aggregator._cached_dfs == {}
    
    def test_incremental_aggregation(self, business_aggregator):
        """Test l'agrégation incrémentale"""
        # Mock des méthodes d'agrégation
        business_aggregator.aggregate_temporal_patterns = Mock(return_value=Mock())
        business_aggregator.aggregate_geographic_patterns = Mock(return_value=Mock())
        business_aggregator.aggregate_weather_patterns = Mock(return_value=Mock())
        business_aggregator.aggregate_infrastructure_patterns = Mock(return_value=Mock())
        business_aggregator._clear_cache = Mock()
        
        result = business_aggregator.incremental_aggregation("2024-01-01T00:00:00")
        
        assert len(result) == 4
        assert 'temporal' in result
        assert 'geographic' in result
        assert 'weather' in result
        assert 'infrastructure' in result
        business_aggregator._clear_cache.assert_called_once()


class TestKPICalculator:
    """Tests pour KPICalculator"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession"""
        return Mock(spec=SparkSession)
    
    @pytest.fixture
    def mock_config_manager(self):
        """Mock ConfigManager"""
        config = Mock(spec=ConfigManager)
        config.get.return_value = "test_database"
        return config
    
    @pytest.fixture
    def mock_logger(self):
        """Mock Logger"""
        return Mock(spec=Logger)
    
    @pytest.fixture
    def kpi_calculator(self, mock_spark, mock_config_manager, mock_logger):
        """Instance KPICalculator pour tests"""
        return KPICalculator(mock_spark, mock_config_manager, mock_logger)
    
    @pytest.fixture
    def sample_aggregated_data(self):
        """Données agrégées simulées"""
        mock_df = Mock(spec=DataFrame)
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.union.return_value = mock_df
        mock_df.count.return_value = 100
        
        return {
            'geographic': mock_df,
            'temporal': mock_df,
            'infrastructure': mock_df,
            'weather': mock_df
        }
    
    def test_kpi_calculator_initialization(self, kpi_calculator):
        """Test l'initialisation de KPICalculator"""
        assert kpi_calculator.hive_database == "test_database"
        assert 'high_danger_threshold' in kpi_calculator.kpi_thresholds
        assert 'hotspot_min_accidents' in kpi_calculator.kpi_thresholds
    
    def test_calculate_security_kpis(self, kpi_calculator, sample_aggregated_data):
        """Test le calcul des KPIs de sécurité"""
        result = kpi_calculator.calculate_security_kpis(sample_aggregated_data)
        
        assert result is not None
        # Vérifier que les méthodes DataFrame ont été appelées
        sample_aggregated_data['geographic'].filter.assert_called()
    
    def test_calculate_temporal_kpis(self, kpi_calculator, sample_aggregated_data):
        """Test le calcul des KPIs temporels"""
        result = kpi_calculator.calculate_temporal_kpis(sample_aggregated_data)
        
        assert result is not None
        sample_aggregated_data['temporal'].select.assert_called()
    
    def test_calculate_infrastructure_kpis(self, kpi_calculator, sample_aggregated_data):
        """Test le calcul des KPIs d'infrastructure"""
        result = kpi_calculator.calculate_infrastructure_kpis(sample_aggregated_data)
        
        assert result is not None
        sample_aggregated_data['infrastructure'].select.assert_called()
    
    def test_calculate_hotspots(self, kpi_calculator, sample_aggregated_data):
        """Test le calcul des hotspots"""
        result = kpi_calculator.calculate_hotspots(sample_aggregated_data)
        
        assert result is not None
        sample_aggregated_data['geographic'].filter.assert_called()
    
    def test_calculate_ml_performance_kpis(self, kpi_calculator):
        """Test le calcul des KPIs de performance ML"""
        result = kpi_calculator.calculate_ml_performance_kpis()
        
        # Doit retourner des données simulées
        assert result is not None
    
    def test_calculate_security_kpis_missing_data(self, kpi_calculator):
        """Test le calcul des KPIs de sécurité avec données manquantes"""
        with pytest.raises(DataValidationError):
            kpi_calculator.calculate_security_kpis({})
    
    def test_incremental_kpi_update(self, kpi_calculator, sample_aggregated_data):
        """Test la mise à jour incrémentale des KPIs"""
        # Mock des méthodes de calcul
        kpi_calculator.calculate_security_kpis = Mock(return_value=Mock())
        kpi_calculator.calculate_temporal_kpis = Mock(return_value=Mock())
        kpi_calculator.calculate_infrastructure_kpis = Mock(return_value=Mock())
        kpi_calculator.calculate_hotspots = Mock(return_value=Mock())
        kpi_calculator.calculate_ml_performance_kpis = Mock(return_value=Mock())
        
        result = kpi_calculator.incremental_kpi_update(sample_aggregated_data)
        
        assert len(result) >= 4  # Au moins 4 types de KPIs
        kpi_calculator.calculate_security_kpis.assert_called_once()
        kpi_calculator.calculate_temporal_kpis.assert_called_once()


class TestMySQLExporter:
    """Tests pour MySQLExporter"""
    
    @pytest.fixture
    def mock_db_connector(self):
        """Mock DatabaseConnector"""
        connector = Mock(spec=DatabaseConnector)
        connector.create_table = Mock()
        connector.create_indexes = Mock()
        connector.bulk_insert = Mock(return_value=100)
        return connector
    
    @pytest.fixture
    def mock_config_manager(self):
        """Mock ConfigManager"""
        config = Mock(spec=ConfigManager)
        config.get.return_value = 1000
        return config
    
    @pytest.fixture
    def mock_logger(self):
        """Mock Logger"""
        return Mock(spec=Logger)
    
    @pytest.fixture
    def mysql_exporter(self, mock_db_connector, mock_config_manager, mock_logger):
        """Instance MySQLExporter pour tests"""
        return MySQLExporter(mock_db_connector, mock_config_manager, mock_logger)
    
    @pytest.fixture
    def sample_kpis_data(self):
        """Données KPIs simulées"""
        mock_df = Mock(spec=DataFrame)
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.toPandas.return_value = pd.DataFrame({
            'state': ['CA', 'TX'],
            'city': ['Los Angeles', 'Houston'],
            'accidents_count': [100, 80],
            'danger_index': [2.5, 2.1]
        })
        mock_df.count.return_value = 2
        
        return {
            'security': mock_df,
            'temporal': mock_df,
            'infrastructure': mock_df,
            'hotspots': mock_df
        }
    
    def test_mysql_exporter_initialization(self, mysql_exporter):
        """Test l'initialisation de MySQLExporter"""
        assert mysql_exporter.batch_size == 1000
        assert mysql_exporter.max_retries == 3
        assert len(mysql_exporter.table_schemas) == 6
    
    def test_create_gold_tables(self, mysql_exporter):
        """Test la création des tables Gold"""
        mysql_exporter.create_gold_tables()
        
        # Vérifier que create_table a été appelé pour chaque table
        assert mysql_exporter.db_connector.create_table.call_count == 6
    
    def test_export_accidents_summary(self, mysql_exporter, sample_kpis_data):
        """Test l'export de la table accidents_summary"""
        result = mysql_exporter.export_accidents_summary(sample_kpis_data)
        
        assert result['table'] == 'accidents_summary'
        assert result['rows_exported'] == 100
        assert result['status'] == 'SUCCESS'
        mysql_exporter.db_connector.bulk_insert.assert_called_once()
    
    def test_export_kpi_data(self, mysql_exporter, sample_kpis_data):
        """Test l'export des données KPI"""
        result = mysql_exporter.export_kpi_data('security', sample_kpis_data['security'])
        
        assert result['table'] == 'kpis_security'
        assert result['kpi_type'] == 'security'
        assert result['rows_exported'] == 100
        assert result['status'] == 'SUCCESS'
    
    def test_export_kpi_data_unknown_type(self, mysql_exporter, sample_kpis_data):
        """Test l'export avec type KPI inconnu"""
        with pytest.raises(DataValidationError):
            mysql_exporter.export_kpi_data('unknown', sample_kpis_data['security'])
    
    def test_incremental_export(self, mysql_exporter, sample_kpis_data):
        """Test l'export incrémental"""
        mysql_exporter.export_kpi_data = Mock(return_value={'rows_exported': 50, 'status': 'SUCCESS'})
        
        result = mysql_exporter.incremental_export(sample_kpis_data)
        
        assert result['export_type'] == 'incremental'
        assert result['total_rows_exported'] == 200  # 4 types * 50 rows
        assert result['status'] == 'SUCCESS'
    
    def test_clean_data_for_mysql(self, mysql_exporter):
        """Test le nettoyage des données pour MySQL"""
        df = pd.DataFrame({
            'text_col': ['test' * 200, None],  # Texte long et valeur nulle
            'float_col': [1.123456789, 2.987654321],
            'int_col': [1, 2]
        })
        
        cleaned_df = mysql_exporter._clean_data_for_mysql(df)
        
        # Vérifier la limitation de longueur
        assert len(cleaned_df['text_col'].iloc[0]) <= 500
        # Vérifier l'arrondi des flottants
        assert cleaned_df['float_col'].iloc[0] == 1.123457
        # Vérifier la gestion des valeurs nulles
        assert cleaned_df['text_col'].iloc[1] is None


class TestTableOptimizer:
    """Tests pour TableOptimizer"""
    
    @pytest.fixture
    def mock_db_connector(self):
        """Mock DatabaseConnector"""
        connector = Mock(spec=DatabaseConnector)
        connector.create_indexes = Mock()
        connector.execute_non_query = Mock()
        connector.execute_query = Mock(return_value=[])
        connector.get_table_info = Mock(return_value={'stats': {'TABLE_ROWS': 1000}})
        return connector
    
    @pytest.fixture
    def mock_config_manager(self):
        """Mock ConfigManager"""
        return Mock(spec=ConfigManager)
    
    @pytest.fixture
    def mock_logger(self):
        """Mock Logger"""
        return Mock(spec=Logger)
    
    @pytest.fixture
    def table_optimizer(self, mock_db_connector, mock_config_manager, mock_logger):
        """Instance TableOptimizer pour tests"""
        return TableOptimizer(mock_db_connector, mock_config_manager, mock_logger)
    
    def test_table_optimizer_initialization(self, table_optimizer):
        """Test l'initialisation de TableOptimizer"""
        assert len(table_optimizer.gold_tables) == 6
        assert 'hotspots_endpoint' in table_optimizer.api_query_patterns
    
    def test_create_optimal_indexes(self, table_optimizer):
        """Test la création d'index optimaux"""
        result = table_optimizer.create_optimal_indexes()
        
        assert 'total_indexes_created' in result
        assert 'tables_processed' in result
        assert result['tables_processed'] == 6
    
    def test_optimize_query_performance(self, table_optimizer):
        """Test l'optimisation des performances"""
        result = table_optimizer.optimize_query_performance()
        
        assert 'slow_queries_analysis' in result
        assert 'statistics_update' in result
        assert 'mysql_configuration' in result
        assert 'execution_plans' in result
        assert result['status'] == 'SUCCESS'
    
    def test_analyze_table_statistics(self, table_optimizer):
        """Test l'analyse des statistiques"""
        result = table_optimizer.analyze_table_statistics()
        
        assert 'tables_analyzed' in result
        assert 'statistics' in result
        assert result['tables_analyzed'] == 6
    
    def test_generate_optimization_recommendations(self, table_optimizer):
        """Test la génération de recommandations"""
        table_optimizer._analyze_table_for_recommendations = Mock(return_value=[
            {'priority': 1, 'type': 'TEST'}
        ])
        
        recommendations = table_optimizer.generate_optimization_recommendations()
        
        assert len(recommendations) >= 3  # Au moins les recommandations globales
        # Vérifier le tri par priorité
        assert recommendations[0]['priority'] <= recommendations[-1]['priority']
    
    def test_get_optimal_indexes_for_table(self, table_optimizer):
        """Test la récupération d'index optimaux pour une table"""
        indexes = table_optimizer._get_optimal_indexes_for_table('accidents_summary')
        
        assert len(indexes) > 0
        assert all('name' in idx and 'columns' in idx for idx in indexes)
    
    def test_analyze_table_for_recommendations_large_table(self, table_optimizer):
        """Test les recommandations pour une grande table"""
        table_optimizer.db_connector.get_table_info.return_value = {
            'stats': {
                'TABLE_ROWS': 2000000,  # Plus d'1M de lignes
                'DATA_LENGTH': 1000000,
                'INDEX_LENGTH': 50000
            }
        }
        
        recommendations = table_optimizer._analyze_table_for_recommendations('test_table')
        
        # Doit recommander le partitioning
        assert any(rec['type'] == 'PARTITIONING' for rec in recommendations)


class TestAnalyticsEngine:
    """Tests pour AnalyticsEngine"""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession"""
        spark = Mock(spec=SparkSession)
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ['col1', 'col2', 'col3']
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        spark.table.return_value = mock_df
        return spark
    
    @pytest.fixture
    def mock_config_manager(self):
        """Mock ConfigManager"""
        config = Mock(spec=ConfigManager)
        config.get.return_value = "test_database"
        return config
    
    @pytest.fixture
    def mock_logger(self):
        """Mock Logger"""
        return Mock(spec=Logger)
    
    @pytest.fixture
    def analytics_engine(self, mock_spark, mock_config_manager, mock_logger):
        """Instance AnalyticsEngine pour tests"""
        return AnalyticsEngine(mock_spark, mock_config_manager, mock_logger)
    
    @pytest.fixture
    def sample_aggregated_data(self):
        """Données agrégées simulées"""
        mock_df = Mock(spec=DataFrame)
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.collect.return_value = [{'mean_accidents': 100, 'stddev_accidents': 20}]
        mock_df.count.return_value = 100
        
        return {
            'temporal': mock_df,
            'geographic': mock_df,
            'weather': mock_df,
            'infrastructure': mock_df
        }
    
    def test_analytics_engine_initialization(self, analytics_engine):
        """Test l'initialisation de AnalyticsEngine"""
        assert analytics_engine.hive_database == "test_database"
        assert 'correlation_significance' in analytics_engine.analysis_thresholds
        assert 'anomaly_threshold' in analytics_engine.analysis_thresholds
    
    def test_calculate_data_quality_metrics(self, analytics_engine):
        """Test le calcul des métriques de qualité"""
        analytics_engine._calculate_table_quality_metrics = Mock(return_value={
            'total_rows': 1000,
            'completeness_percentage': 95.0,
            'quality_score': 90.0
        })
        
        result = analytics_engine.calculate_data_quality_metrics()
        
        assert 'global' in result
        assert len(result) == 4  # 3 tables + global
    
    def test_detect_anomalies(self, analytics_engine, sample_aggregated_data):
        """Test la détection d'anomalies"""
        analytics_engine._detect_temporal_anomalies = Mock(return_value=[
            {'type': 'temporal_accident_count', 'severity': 'HIGH'}
        ])
        analytics_engine._detect_geographic_anomalies = Mock(return_value=[])
        analytics_engine._detect_weather_anomalies = Mock(return_value=[])
        
        result = analytics_engine.detect_anomalies(sample_aggregated_data)
        
        assert 'temporal' in result
        assert 'geographic' in result
        assert 'weather' in result
        assert 'summary' in result
    
    def test_calculate_correlations(self, analytics_engine, sample_aggregated_data):
        """Test le calcul des corrélations"""
        analytics_engine._calculate_weather_correlations = Mock(return_value={
            'temperature_accidents': {'correlation': 0.5, 'significance': 'HIGH'}
        })
        analytics_engine._calculate_infrastructure_correlations = Mock(return_value={})
        analytics_engine._calculate_temporal_correlations = Mock(return_value={})
        analytics_engine._calculate_cross_correlations = Mock(return_value={})
        
        result = analytics_engine.calculate_correlations(sample_aggregated_data)
        
        assert 'weather_accidents' in result
        assert 'infrastructure_safety' in result
        assert 'temporal_patterns' in result
    
    def test_generate_insights(self, analytics_engine):
        """Test la génération d'insights"""
        sample_kpis = {'security': Mock(), 'temporal': Mock(), 'infrastructure': Mock()}
        sample_correlations = {'weather_accidents': {'temp_corr': {'significance': 'HIGH'}}}
        sample_anomalies = {'summary': {'requires_attention': True, 'high_severity_anomalies': 2}}
        
        analytics_engine._generate_security_insights = Mock(return_value=[
            {'type': 'security', 'priority': 1}
        ])
        analytics_engine._generate_temporal_insights = Mock(return_value=[])
        analytics_engine._generate_infrastructure_insights = Mock(return_value=[])
        analytics_engine._generate_correlation_insights = Mock(return_value=[])
        analytics_engine._generate_anomaly_insights = Mock(return_value=[
            {'type': 'anomaly', 'priority': 1}
        ])
        
        result = analytics_engine.generate_insights(sample_kpis, sample_correlations, sample_anomalies)
        
        assert len(result) >= 2
        # Vérifier le tri par priorité
        assert all(result[i].get('priority', 5) <= result[i+1].get('priority', 5)
                  for i in range(len(result)-1))
    
    def test_calculate_table_quality_metrics(self, analytics_engine):
        """Test le calcul des métriques de qualité d'une table"""
        # Mock DataFrame avec données de test
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.columns = ['id', 'start_lat', 'start_lng', 'severity']
        mock_df.filter.return_value.count.side_effect = [10, 5, 0, 2]  # Valeurs nulles par colonne
        mock_df.dropDuplicates.return_value.count.return_value = 995  # 5 doublons
        
        result = analytics_engine._calculate_table_quality_metrics(mock_df, 'test_table')
        
        assert result['total_rows'] == 1000
        assert result['total_columns'] == 4
        assert 'completeness_percentage' in result
        assert 'quality_score' in result
        assert result['duplicate_count'] == 5
    
    def test_calculate_quality_score(self, analytics_engine):
        """Test le calcul du score de qualité"""
        score = analytics_engine._calculate_quality_score(
            completeness=95.0,
            duplicates=10,
            consistency_issues=2,
            total_rows=1000
        )
        
        assert 0 <= score <= 100
        assert score < 95  # Doit être pénalisé pour les doublons et problèmes
    
    def test_detect_temporal_anomalies(self, analytics_engine):
        """Test la détection d'anomalies temporelles"""
        mock_df = Mock()
        mock_df.select.return_value.collect.return_value = [{
            'mean_accidents': 100,
            'stddev_accidents': 20,
            'mean_severity': 2.5,
            'stddev_severity': 0.5
        }]
        mock_df.filter.return_value.collect.return_value = [
            {
                'period_type': 'hourly',
                'period_value': '18',
                'accidents_count': 200,  # Anomalie
                'avg_severity': 3.5
            }
        ]
        
        result = analytics_engine._detect_temporal_anomalies(mock_df)
        
        assert len(result) >= 1
        assert result[0]['type'] == 'temporal_accident_count'
        assert result[0]['severity'] in ['HIGH', 'MEDIUM']


if __name__ == "__main__":
    """Exécution des tests"""
    pytest.main([__file__, "-v", "--tb=short"])