"""
Application principale PREPROCESSOR pour la transformation et nettoyage des données US-Accidents
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from ...common.config.config_manager import ConfigManager
from ...common.config.spark_config import SparkConfig
from ...common.utils.logger import Logger
from ...common.utils.spark_utils import SparkUtils
from ...common.exceptions.custom_exceptions import (
    LakehouseException, SparkJobError, FileProcessingError, DataValidationError
)

from .data_cleaner import DataCleaner
from .feature_engineer import FeatureEngineer
from .hive_table_manager import HiveTableManager
from .transformation_factory import TransformationFactory
from .aggregation_engine import AggregationEngine


class PreprocessorApp:
    """
    Application principale PREPROCESSOR pour la transformation des données US-Accidents
    
    Fonctionnalités:
    - Lecture données Bronze (HDFS Parquet partitionné)
    - Nettoyage et normalisation des 47 colonnes US-Accidents
    - Feature engineering avancé (temporel, météo, géographique, infrastructure)
    - Création tables Hive optimisées (accidents_clean, weather_aggregated, infrastructure_features)
    - Optimisations Spark (bucketing, ORC, cache, broadcast)
    - Logging détaillé des métriques de transformation (JSON structuré)
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise l'application PREPROCESSOR"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("preprocessor", self.config_manager)
        self.spark_utils = SparkUtils(self.config_manager)
        
        # Configuration spécifique
        self.bronze_path = self.config_manager.get_hdfs_path('bronze')
        self.silver_path = self.config_manager.get_hdfs_path('silver')
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Composants spécialisés
        self.data_cleaner = DataCleaner(self.config_manager)
        self.feature_engineer = FeatureEngineer(self.config_manager)
        self.hive_table_manager = HiveTableManager(self.config_manager)
        self.transformation_factory = TransformationFactory(self.config_manager)
        self.aggregation_engine = AggregationEngine(self.config_manager)
        
        # Session Spark
        self.spark: Optional[SparkSession] = None
        
        # Métriques d'exécution
        self.execution_metrics = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'records_read': 0,
            'records_processed': 0,
            'records_written': 0,
            'features_created': 0,
            'tables_created': 0,
            'cleaning_score': 0.0,
            'transformation_score': 0.0,
            'errors_count': 0,
            'retry_count': 0
        }
    
    def run(self) -> Dict[str, Any]:
        """
        Exécute le pipeline de preprocessing complet avec retry automatique
        
        Returns:
            Dict contenant les métriques d'exécution et le statut
        """
        self.execution_metrics['start_time'] = datetime.now()
        
        try:
            self.logger.log_startup(
                app_name="preprocessor",
                version="1.0.0",
                config_summary={
                    'bronze_path': self.bronze_path,
                    'silver_path': self.silver_path,
                    'max_retries': self.max_retries
                }
            )
            
            # Initialisation de Spark
            self._initialize_spark()
            
            # Exécution avec retry
            result = self._execute_with_retry()
            
            # Finalisation
            self._finalize_execution()
            
            return result
            
        except Exception as e:
            self.logger.error("PREPROCESSOR application failed", exception=e)
            self.execution_metrics['errors_count'] += 1
            return self._create_error_result(e)
        
        finally:
            self._cleanup()
    
    def _execute_with_retry(self) -> Dict[str, Any]:
        """Exécute le pipeline avec retry automatique"""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                self.execution_metrics['retry_count'] = attempt
                
                if attempt > 0:
                    self.logger.info(f"Retry attempt {attempt + 1}/{self.max_retries}")
                    time.sleep(self.retry_delay * attempt)  # Backoff exponentiel
                
                return self._execute_pipeline()
                
            except Exception as e:
                last_exception = e
                self.logger.warning(
                    f"Pipeline execution failed on attempt {attempt + 1}",
                    exception=e,
                    attempt=attempt + 1,
                    max_retries=self.max_retries
                )
                
                if attempt < self.max_retries - 1:
                    continue
                else:
                    break
        
        # Toutes les tentatives ont échoué
        raise SparkJobError(
            message=f"Pipeline failed after {self.max_retries} attempts",
            job_name="preprocessor_pipeline",
            stage="execution_with_retry",
            spark_error=str(last_exception)
        )
    
    def _execute_pipeline(self) -> Dict[str, Any]:
        """Exécute le pipeline de preprocessing principal"""
        try:
            # Étape 1: Lecture des données Bronze
            self.logger.log_spark_job("preprocessor_pipeline", "bronze_data_reading_start")
            bronze_df = self._read_bronze_data()
            self.execution_metrics['records_read'] = bronze_df.count()
            
            # Étape 2: Nettoyage des données
            self.logger.log_spark_job("preprocessor_pipeline", "data_cleaning_start")
            cleaned_df = self.data_cleaner.clean_data(bronze_df)
            cleaning_metrics = self.data_cleaner.get_cleaning_metrics()
            self.execution_metrics['cleaning_score'] = cleaning_metrics.get('cleaning_score', 0.0)
            
            # Étape 3: Feature Engineering
            self.logger.log_spark_job("preprocessor_pipeline", "feature_engineering_start")
            enriched_df = self.feature_engineer.create_features(cleaned_df)
            feature_metrics = self.feature_engineer.get_feature_metrics()
            self.execution_metrics['features_created'] = feature_metrics.get('features_created', 0)
            
            # Étape 4: Optimisation et cache
            self.logger.log_spark_job("preprocessor_pipeline", "optimization_start")
            optimized_df = self._optimize_dataframe(enriched_df)
            
            # Étape 5: Création des tables Hive
            self.logger.log_spark_job("preprocessor_pipeline", "hive_tables_creation_start")
            tables_result = self._create_hive_tables(optimized_df)
            self.execution_metrics['tables_created'] = tables_result['tables_created']
            self.execution_metrics['records_written'] = tables_result['total_records_written']
            
            # Étape 6: Agrégations
            self.logger.log_spark_job("preprocessor_pipeline", "aggregations_start")
            aggregation_result = self._create_aggregations(optimized_df)
            
            # Métriques finales
            self._calculate_final_metrics()
            
            return self._create_success_result()
            
        except Exception as e:
            self.logger.error("Pipeline execution failed", exception=e)
            raise
    
    def _initialize_spark(self):
        """Initialise la session Spark avec configuration optimisée"""
        try:
            self.spark = self.spark_utils.create_spark_session(
                app_name="lakehouse-accidents-preprocessor",
                app_type="preprocessor"
            )
            
            # Configuration spécifique pour le preprocessing
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            self.spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
            
            # Configuration pour ORC
            self.spark.conf.set("spark.sql.orc.impl", "native")
            self.spark.conf.set("spark.sql.orc.enableVectorizedReader", "true")
            
            # Configuration pour bucketing
            self.spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
            self.spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true")
            
            self.logger.info("Spark session initialized for PREPROCESSOR", 
                           spark_version=self.spark.version,
                           app_id=self.spark.sparkContext.applicationId)
            
        except Exception as e:
            raise SparkJobError(
                message="Failed to initialize Spark session",
                job_name="preprocessor_initialization",
                stage="spark_session_creation",
                spark_error=str(e)
            )
    
    def _read_bronze_data(self) -> DataFrame:
        """Lit les données depuis la couche Bronze"""
        try:
            bronze_df = self.spark_utils.read_optimized_data(
                path=self.bronze_path,
                format="parquet"
            )
            
            self.logger.info("Bronze data loaded", 
                           path=self.bronze_path,
                           records_count=bronze_df.count(),
                           columns_count=len(bronze_df.columns))
            
            return bronze_df
            
        except Exception as e:
            raise FileProcessingError(
                message="Failed to read Bronze data",
                file_path=self.bronze_path,
                file_format="parquet",
                operation="read"
            )
    
    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """Applique les optimisations Spark au DataFrame"""
        try:
            # Cache intelligent
            cached_df = self.spark_utils.apply_intelligent_caching(
                df, 
                cache_level="MEMORY_AND_DISK",
                force_cache=False
            )
            
            # Optimisation du partitioning
            optimized_df = self.spark_utils.optimize_dataframe_partitioning(
                cached_df,
                partition_columns=['State'],
                target_partition_size_mb=128
            )
            
            self.logger.log_performance(
                operation="dataframe_optimization",
                duration_seconds=0,
                original_partitions=df.rdd.getNumPartitions(),
                optimized_partitions=optimized_df.rdd.getNumPartitions()
            )
            
            return optimized_df
            
        except Exception as e:
            self.logger.warning("DataFrame optimization failed, using original", exception=e)
            return df
    
    def _create_hive_tables(self, df: DataFrame) -> Dict[str, Any]:
        """Crée les tables Hive optimisées"""
        try:
            tables_created = 0
            total_records_written = 0
            
            # Table principale accidents_clean
            accidents_result = self.hive_table_manager.create_accidents_clean_table(df)
            if accidents_result['success']:
                tables_created += 1
                total_records_written += accidents_result['records_written']
            
            # Table weather_aggregated
            weather_result = self.hive_table_manager.create_weather_aggregated_table(df)
            if weather_result['success']:
                tables_created += 1
                total_records_written += weather_result['records_written']
            
            # Table infrastructure_features
            infra_result = self.hive_table_manager.create_infrastructure_features_table(df)
            if infra_result['success']:
                tables_created += 1
                total_records_written += infra_result['records_written']
            
            self.logger.log_performance(
                operation="hive_tables_creation",
                duration_seconds=0,
                tables_created=tables_created,
                total_records_written=total_records_written
            )
            
            return {
                'tables_created': tables_created,
                'total_records_written': total_records_written,
                'accidents_result': accidents_result,
                'weather_result': weather_result,
                'infrastructure_result': infra_result
            }
            
        except Exception as e:
            self.logger.error("Hive tables creation failed", exception=e)
            raise
    
    def _create_aggregations(self, df: DataFrame) -> Dict[str, Any]:
        """Crée les agrégations avancées"""
        try:
            aggregation_results = self.aggregation_engine.create_all_aggregations(df)
            
            self.logger.log_performance(
                operation="aggregations_creation",
                duration_seconds=0,
                aggregations_created=len(aggregation_results),
                aggregation_types=list(aggregation_results.keys())
            )
            
            return aggregation_results
            
        except Exception as e:
            self.logger.error("Aggregations creation failed", exception=e)
            return {}
    
    def _calculate_final_metrics(self):
        """Calcule les métriques finales d'exécution"""
        self.execution_metrics['end_time'] = datetime.now()
        
        if self.execution_metrics['start_time']:
            duration = self.execution_metrics['end_time'] - self.execution_metrics['start_time']
            self.execution_metrics['duration_seconds'] = duration.total_seconds()
        
        # Calcul du score de transformation global
        cleaning_score = self.execution_metrics.get('cleaning_score', 0.0)
        features_created = self.execution_metrics.get('features_created', 0)
        tables_created = self.execution_metrics.get('tables_created', 0)
        
        transformation_score = (
            (cleaning_score * 0.4) +
            (min(features_created / 20, 1.0) * 100 * 0.4) +
            (min(tables_created / 3, 1.0) * 100 * 0.2)
        )
        
        self.execution_metrics['transformation_score'] = round(transformation_score, 2)
        
        # Métriques Spark
        if self.spark:
            spark_metrics = self.spark_utils.get_spark_metrics(self.spark)
            self.execution_metrics.update(spark_metrics)
    
    def _finalize_execution(self):
        """Finalise l'exécution avec logging des métriques"""
        self.logger.log_performance(
            operation="preprocessor_pipeline_complete",
            duration_seconds=self.execution_metrics['duration_seconds'],
            records_read=self.execution_metrics['records_read'],
            records_processed=self.execution_metrics['records_processed'],
            records_written=self.execution_metrics['records_written'],
            features_created=self.execution_metrics['features_created'],
            tables_created=self.execution_metrics['tables_created'],
            cleaning_score=self.execution_metrics['cleaning_score'],
            transformation_score=self.execution_metrics['transformation_score'],
            retry_count=self.execution_metrics['retry_count']
        )
        
        self.logger.log_business_event(
            event_type="data_preprocessing_completed",
            event_data={
                'source': 'bronze_layer',
                'destination': 'silver_layer',
                'records_processed': self.execution_metrics['records_written'],
                'features_created': self.execution_metrics['features_created'],
                'tables_created': self.execution_metrics['tables_created'],
                'transformation_score': self.execution_metrics['transformation_score']
            }
        )
    
    def _create_success_result(self) -> Dict[str, Any]:
        """Crée le résultat de succès"""
        return {
            'status': 'success',
            'message': 'PREPROCESSOR pipeline completed successfully',
            'metrics': self.execution_metrics,
            'timestamp': datetime.now().isoformat()
        }
    
    def _create_error_result(self, exception: Exception) -> Dict[str, Any]:
        """Crée le résultat d'erreur"""
        return {
            'status': 'error',
            'message': str(exception),
            'error_type': type(exception).__name__,
            'metrics': self.execution_metrics,
            'timestamp': datetime.now().isoformat()
        }
    
    def _cleanup(self):
        """Nettoie les ressources"""
        try:
            if self.spark:
                uptime = self.execution_metrics.get('duration_seconds', 0)
                self.logger.log_shutdown(
                    app_name="preprocessor",
                    uptime_seconds=uptime,
                    final_metrics=self.execution_metrics
                )
                
                self.spark_utils.cleanup_spark_session(self.spark)
                self.spark = None
                
        except Exception as e:
            self.logger.error("Cleanup failed", exception=e)


def main():
    """Point d'entrée principal pour l'application PREPROCESSOR"""
    try:
        preprocessor = PreprocessorApp()
        result = preprocessor.run()
        
        if result['status'] == 'success':
            print(f"✅ PREPROCESSOR completed successfully: {result['metrics']['records_written']} records processed")
            print(f"📊 Features created: {result['metrics']['features_created']}")
            print(f"🗃️ Tables created: {result['metrics']['tables_created']}")
            print(f"⭐ Transformation score: {result['metrics']['transformation_score']:.2f}%")
            exit(0)
        else:
            print(f"❌ PREPROCESSOR failed: {result['message']}")
            exit(1)
            
    except Exception as e:
        print(f"💥 PREPROCESSOR crashed: {str(e)}")
        exit(2)


if __name__ == "__main__":
    main()