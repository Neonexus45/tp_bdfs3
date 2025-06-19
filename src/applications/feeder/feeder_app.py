"""
Application principale FEEDER pour l'ingestion quotidienne des données US-Accidents
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

from .data_ingestion import DataIngestion
from .schema_validator import SchemaValidator
from .quality_checker import QualityChecker
from .partitioning_strategy import PartitioningStrategy


class FeederApp:
    """
    Application principale FEEDER pour l'ingestion des données US-Accidents
    
    Fonctionnalités:
    - Lecture CSV avec schéma strict des 47 colonnes exactes
    - Validation qualité des données (nulls, types, coordonnées GPS)
    - Gestion erreurs et données corrompues avec logging détaillé
    - Partitioning intelligent par date d'ingestion et état
    - Compression et optimisation stockage Parquet (Snappy)
    - Logging détaillé des métriques d'ingestion (JSON structuré)
    - Optimisations Spark (coalesce, partition pruning, cache)
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise l'application FEEDER"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("feeder", self.config_manager)
        self.spark_utils = SparkUtils(self.config_manager)
        
        # Configuration spécifique
        self.source_path = self.config_manager.get('data.source_path')
        self.bronze_path = self.config_manager.get_hdfs_path('bronze')
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Composants spécialisés
        self.data_ingestion = DataIngestion(self.config_manager)
        self.schema_validator = SchemaValidator(self.config_manager)
        self.quality_checker = QualityChecker(self.config_manager)
        self.partitioning_strategy = PartitioningStrategy(self.config_manager)
        
        # Session Spark
        self.spark: Optional[SparkSession] = None
        
        # Métriques d'exécution
        self.execution_metrics = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'records_read': 0,
            'records_written': 0,
            'partitions_created': 0,
            'quality_score': 0.0,
            'errors_count': 0,
            'retry_count': 0
        }
    
    def run(self) -> Dict[str, Any]:
        """
        Exécute le pipeline d'ingestion complet avec retry automatique
        
        Returns:
            Dict contenant les métriques d'exécution et le statut
        """
        self.execution_metrics['start_time'] = datetime.now()
        
        try:
            self.logger.log_startup(
                app_name="feeder",
                version="1.0.0",
                config_summary={
                    'source_path': self.source_path,
                    'bronze_path': self.bronze_path,
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
            self.logger.error("FEEDER application failed", exception=e)
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
            job_name="feeder_pipeline",
            stage="execution_with_retry",
            spark_error=str(last_exception)
        )
    
    def _execute_pipeline(self) -> Dict[str, Any]:
        """Exécute le pipeline d'ingestion principal"""
        try:
            # Étape 1: Lecture des données
            self.logger.info("Starting stage: Data Reading")
            self.logger.log_spark_job("feeder_pipeline", "data_reading_start")
            raw_df = self.data_ingestion.read_csv_data(self.source_path)
            self.execution_metrics['records_read'] = raw_df.count()
            self.logger.info("Completed stage: Data Reading")
            
            # Étape 2: Validation du schéma
            self.logger.info("Starting stage: Schema Validation")
            self.logger.log_spark_job("feeder_pipeline", "schema_validation_start")
            schema_result = self.schema_validator.validate_dataframe_schema(raw_df)
            
            # Access the validation result from the nested structure
            validation_result = schema_result.get('schema_validation', schema_result)
            if not validation_result.get('is_valid', True):
                raise DataValidationError(
                    message="Schema validation failed",
                    validation_type="schema",
                    failed_rules=validation_result.get('issues', []),
                    dataset="us_accidents"
                )
            self.logger.info("Completed stage: Schema Validation")
            
            # Étape 3: Contrôle qualité
            self.logger.info("Starting stage: Quality Check")
            self.logger.log_spark_job("feeder_pipeline", "quality_check_start")
            quality_result = self.quality_checker.check_data_quality(raw_df)
            self.execution_metrics['quality_score'] = quality_result.get('quality_score', 0.0)
            self.logger.info("Completed stage: Quality Check")
            
            # Étape 4: Nettoyage et préparation
            self.logger.info("Starting stage: Data Preparation")
            self.logger.log_spark_job("feeder_pipeline", "data_preparation_start")
            cleaned_df = self.quality_checker.clean_corrupted_data(raw_df)
            self.logger.info("Completed stage: Data Preparation")
            
            # Étape 5: Optimisation et cache
            self.logger.info("Starting stage: Optimization")
            self.logger.log_spark_job("feeder_pipeline", "optimization_start")
            optimized_df = self._optimize_dataframe(cleaned_df)
            self.logger.info("Completed stage: Optimization")
            
            # Étape 6: Partitioning et écriture
            self.logger.info("Starting stage: Partitioning")
            self.logger.log_spark_job("feeder_pipeline", "partitioning_start")
            partitioned_df = self.partitioning_strategy.apply_partitioning(optimized_df)
            self.logger.info("Completed stage: Partitioning")
            
            # Étape 7: Écriture finale
            self.logger.info("Starting stage: Writing")
            self.logger.log_spark_job("feeder_pipeline", "writing_start")
            write_result = self._write_to_bronze_layer(partitioned_df)
            self.execution_metrics['records_written'] = write_result['records_written']
            self.execution_metrics['partitions_created'] = write_result['partitions_created']
            self.logger.info("Completed stage: Writing")
            
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
                app_name="lakehouse-accidents-feeder",
                app_type="feeder"
            )
            
            # Configuration spécifique pour l'ingestion
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self.spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
            
            self.logger.info("Spark session initialized for FEEDER", 
                           spark_version=self.spark.version,
                           app_id=self.spark.sparkContext.applicationId)
            
        except Exception as e:
            raise SparkJobError(
                message="Failed to initialize Spark session",
                job_name="feeder_initialization",
                stage="spark_session_creation",
                spark_error=str(e)
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
    
    def _write_to_bronze_layer(self, df: DataFrame) -> Dict[str, Any]:
        """Écrit les données dans la couche Bronze avec partitioning"""
        try:
            # Coalescing pour éviter les petits fichiers
            coalesced_df = self.spark_utils.coalesce_small_files(df, target_file_size_mb=128)
            
            # Écriture partitionnée
            partition_columns = self.config_manager.get('data.partition_columns', ['date', 'state'])
            compression = self.config_manager.get('data.compression_codec', 'snappy')
            
            self.spark_utils.write_partitioned_data(
                df=coalesced_df,
                path=self.bronze_path,
                partition_columns=partition_columns,
                format="parquet",
                mode="overwrite",
                compression=compression
            )
            
            # Calcul des métriques d'écriture
            records_written = coalesced_df.count()
            partitions_created = coalesced_df.rdd.getNumPartitions()
            
            self.logger.log_performance(
                operation="bronze_layer_write",
                duration_seconds=0,
                records_written=records_written,
                partitions_created=partitions_created,
                compression=compression
            )
            
            return {
                'records_written': records_written,
                'partitions_created': partitions_created
            }
            
        except Exception as e:
            raise FileProcessingError(
                message="Failed to write to Bronze layer",
                file_path=self.bronze_path,
                file_format="parquet",
                operation="write"
            )
    
    def _calculate_final_metrics(self):
        """Calcule les métriques finales d'exécution"""
        self.execution_metrics['end_time'] = datetime.now()
        
        if self.execution_metrics['start_time']:
            duration = self.execution_metrics['end_time'] - self.execution_metrics['start_time']
            self.execution_metrics['duration_seconds'] = duration.total_seconds()
        
        # Métriques Spark
        if self.spark:
            spark_metrics = self.spark_utils.get_spark_metrics(self.spark)
            self.execution_metrics.update(spark_metrics)
    
    def _finalize_execution(self):
        """Finalise l'exécution avec logging des métriques"""
        self.logger.log_performance(
            operation="feeder_pipeline_complete",
            duration_seconds=self.execution_metrics['duration_seconds'],
            records_read=self.execution_metrics['records_read'],
            records_written=self.execution_metrics['records_written'],
            partitions_created=self.execution_metrics['partitions_created'],
            quality_score=self.execution_metrics['quality_score'],
            retry_count=self.execution_metrics['retry_count']
        )
        
        self.logger.log_business_event(
            event_type="data_ingestion_completed",
            event_data={
                'source': 'us_accidents',
                'destination': 'bronze_layer',
                'records_processed': self.execution_metrics['records_written'],
                'quality_score': self.execution_metrics['quality_score']
            }
        )
    
    def _create_success_result(self) -> Dict[str, Any]:
        """Crée le résultat de succès"""
        return {
            'status': 'success',
            'message': 'FEEDER pipeline completed successfully',
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
                    app_name="feeder",
                    uptime_seconds=uptime,
                    final_metrics=self.execution_metrics
                )
                
                self.spark_utils.cleanup_spark_session(self.spark)
                self.spark = None
                
        except Exception as e:
            self.logger.error("Cleanup failed", exception=e)


def main():
    """Point d'entrée principal pour l'application FEEDER"""
    try:
        feeder = FeederApp()
        result = feeder.run()
        
        if result['status'] == 'success':
            print(f"✅ FEEDER completed successfully: {result['metrics']['records_written']} records processed")
            exit(0)
        else:
            print(f"❌ FEEDER failed: {result['message']}")
            exit(1)
            
    except Exception as e:
        print(f"💥 FEEDER crashed: {str(e)}")
        exit(2)


if __name__ == "__main__":
    main()