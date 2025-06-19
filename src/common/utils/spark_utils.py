from typing import Dict, Any, List, Optional, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, broadcast, coalesce, lit
from pyspark.storagelevel import StorageLevel
import pyspark.sql.functions as F
from ..config.config_manager import ConfigManager
from ..config.spark_config import SparkConfig
from .logger import Logger


class SparkUtils:
    """Utilitaires Spark pour optimisations et opérations communes"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.spark_config = SparkConfig(self.config_manager)
        self.logger = Logger("spark_utils", self.config_manager)
    
    def create_spark_session(self, app_name: str, app_type: str = None) -> SparkSession:
        """Crée une session Spark optimisée"""
        try:
            if app_type:
                spark_conf = self.spark_config.get_config_for_app(app_type)
            else:
                spark_conf = self.spark_config.get_config_for_app(app_name)
            
            builder = SparkSession.builder.appName(app_name)
            
            for key, value in spark_conf.items():
                builder = builder.config(key, value)
            
            hive_config = self.config_manager.get('hive')
            if hive_config:
                builder = builder.config("hive.metastore.uris", hive_config['metastore_uri'])
                builder = builder.enableHiveSupport()
            
            spark = builder.getOrCreate()
            
            self.logger.info("Spark session created", 
                           app_name=app_name, 
                           app_type=app_type,
                           spark_version=spark.version)
            
            return spark
            
        except Exception as e:
            self.logger.error("Failed to create Spark session", exception=e, app_name=app_name)
            raise
    
    def optimize_dataframe_partitioning(self, df: DataFrame, 
                                      partition_columns: List[str],
                                      target_partition_size_mb: int = 128) -> DataFrame:
        """Optimise le partitioning d'un DataFrame"""
        try:
            current_partitions = df.rdd.getNumPartitions()
            df_size_mb = self.estimate_dataframe_size_mb(df)
            
            optimal_partitions = max(1, int(df_size_mb / target_partition_size_mb))
            
            if len(partition_columns) == 1:
                optimized_df = df.repartition(optimal_partitions, col(partition_columns[0]))
            else:
                optimized_df = df.repartition(optimal_partitions, *[col(c) for c in partition_columns])
            
            self.logger.log_performance(
                operation="dataframe_partitioning",
                duration_seconds=0,
                original_partitions=current_partitions,
                optimized_partitions=optimal_partitions,
                partition_columns=partition_columns,
                estimated_size_mb=df_size_mb
            )
            
            return optimized_df
            
        except Exception as e:
            self.logger.error("DataFrame partitioning optimization failed", exception=e)
            return df
    
    def apply_intelligent_caching(self, df: DataFrame, 
                                 cache_level: str = "MEMORY_AND_DISK",
                                 force_cache: bool = False) -> DataFrame:
        """Applique une stratégie de cache intelligente"""
        try:
            df_size_mb = self.estimate_dataframe_size_mb(df)
            
            storage_levels = {
                "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
                "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
                "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER,
                "DISK_ONLY": StorageLevel.DISK_ONLY
            }
            
            if not force_cache:
                if df_size_mb < 100:
                    cache_level = "MEMORY_ONLY"
                elif df_size_mb < 1000:
                    cache_level = "MEMORY_AND_DISK"
                else:
                    cache_level = "MEMORY_AND_DISK_SER"
            
            storage_level = storage_levels.get(cache_level, StorageLevel.MEMORY_AND_DISK)
            cached_df = df.persist(storage_level)
            
            self.logger.info("DataFrame cached",
                           cache_level=cache_level,
                           estimated_size_mb=df_size_mb,
                           partitions=df.rdd.getNumPartitions())
            
            return cached_df
            
        except Exception as e:
            self.logger.error("DataFrame caching failed", exception=e)
            return df
    
    def broadcast_small_dataframe(self, df: DataFrame, 
                                 max_broadcast_size_mb: int = 200) -> DataFrame:
        """Broadcast un DataFrame s'il est suffisamment petit"""
        try:
            df_size_mb = self.estimate_dataframe_size_mb(df)
            
            if df_size_mb <= max_broadcast_size_mb:
                broadcasted_df = broadcast(df)
                self.logger.info("DataFrame broadcasted", 
                               size_mb=df_size_mb,
                               max_size_mb=max_broadcast_size_mb)
                return broadcasted_df
            else:
                self.logger.info("DataFrame too large for broadcast",
                               size_mb=df_size_mb,
                               max_size_mb=max_broadcast_size_mb)
                return df
                
        except Exception as e:
            self.logger.error("DataFrame broadcast failed", exception=e)
            return df
    
    def estimate_dataframe_size_mb(self, df: DataFrame) -> float:
        """Estime la taille d'un DataFrame en MB"""
        try:
            sample_df = df.sample(0.01, seed=42)
            sample_count = sample_df.count()
            
            if sample_count == 0:
                return 0.0
            
            total_count = df.count()
            
            sample_json = sample_df.toJSON().collect()
            sample_size_bytes = sum(len(row.encode('utf-8')) for row in sample_json)
            
            estimated_total_size_bytes = (sample_size_bytes / sample_count) * total_count
            estimated_size_mb = estimated_total_size_bytes / (1024 * 1024)
            
            return round(estimated_size_mb, 2)
            
        except Exception as e:
            self.logger.error("DataFrame size estimation failed", exception=e)
            return 100.0
    
    def optimize_joins(self, left_df: DataFrame, right_df: DataFrame,
                      join_keys: List[str], join_type: str = "inner") -> DataFrame:
        """Optimise une jointure entre deux DataFrames"""
        try:
            left_size = self.estimate_dataframe_size_mb(left_df)
            right_size = self.estimate_dataframe_size_mb(right_df)
            
            if right_size < left_size and right_size <= 200:
                right_df = self.broadcast_small_dataframe(right_df)
                self.logger.info("Right DataFrame broadcasted for join optimization")
            elif left_size < right_size and left_size <= 200:
                left_df = self.broadcast_small_dataframe(left_df)
                self.logger.info("Left DataFrame broadcasted for join optimization")
            
            if len(join_keys) == 1:
                join_condition = left_df[join_keys[0]] == right_df[join_keys[0]]
            else:
                join_conditions = [left_df[key] == right_df[key] for key in join_keys]
                join_condition = join_conditions[0]
                for condition in join_conditions[1:]:
                    join_condition = join_condition & condition
            
            result_df = left_df.join(right_df, join_condition, join_type)
            
            self.logger.log_performance(
                operation="optimized_join",
                duration_seconds=0,
                left_size_mb=left_size,
                right_size_mb=right_size,
                join_keys=join_keys,
                join_type=join_type
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error("Join optimization failed", exception=e)
            raise
    
    def write_partitioned_data(self, df: DataFrame, path: str,
                              partition_columns: List[str],
                              format: str = "parquet",
                              mode: str = "overwrite",
                              compression: str = None) -> None:
        """Écrit des données partitionnées de manière optimisée"""
        try:
            compression = compression or self.config_manager.get('data.compression_codec', 'snappy')
            
            writer = df.write.mode(mode)
            
            if format.lower() == "parquet":
                writer = writer.option("compression", compression)
            elif format.lower() == "delta":
                writer = writer.option("compression", compression)
            
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            writer.format(format).save(path)
            
            self.logger.info("Partitioned data written",
                           path=path,
                           format=format,
                           partition_columns=partition_columns,
                           compression=compression,
                           mode=mode)
            
        except Exception as e:
            self.logger.error("Failed to write partitioned data", exception=e, path=path)
            raise
    
    def read_optimized_data(self, path: str, format: str = "parquet",
                           schema: StructType = None,
                           partition_filters: Dict[str, Any] = None) -> DataFrame:
        """Lit des données avec optimisations"""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                raise RuntimeError("No active Spark session")
            
            reader = spark.read.format(format)
            
            if schema:
                reader = reader.schema(schema)
            
            if format.lower() == "parquet":
                reader = reader.option("mergeSchema", "false")
                reader = reader.option("filterPushdown", "true")
            
            df = reader.load(path)
            
            if partition_filters:
                for column, value in partition_filters.items():
                    if isinstance(value, list):
                        df = df.filter(col(column).isin(value))
                    else:
                        df = df.filter(col(column) == value)
            
            self.logger.info("Optimized data read",
                           path=path,
                           format=format,
                           has_schema=schema is not None,
                           partition_filters=partition_filters)
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to read optimized data", exception=e, path=path)
            raise
    
    def coalesce_small_files(self, df: DataFrame, target_file_size_mb: int = 128) -> DataFrame:
        """Coalesce les petits fichiers pour optimiser le stockage"""
        try:
            current_partitions = df.rdd.getNumPartitions()
            df_size_mb = self.estimate_dataframe_size_mb(df)
            
            optimal_partitions = max(1, int(df_size_mb / target_file_size_mb))
            
            if optimal_partitions < current_partitions:
                coalesced_df = df.coalesce(optimal_partitions)
                
                self.logger.info("Small files coalesced",
                               original_partitions=current_partitions,
                               coalesced_partitions=optimal_partitions,
                               target_file_size_mb=target_file_size_mb)
                
                return coalesced_df
            
            return df
            
        except Exception as e:
            self.logger.error("File coalescing failed", exception=e)
            return df
    
    def get_spark_metrics(self, spark: SparkSession) -> Dict[str, Any]:
        """Récupère les métriques Spark"""
        try:
            sc = spark.sparkContext
            
            metrics = {
                'app_id': sc.applicationId,
                'app_name': sc.appName,
                'spark_version': spark.version,
                'default_parallelism': sc.defaultParallelism,
                'total_executors': len(sc.statusTracker().getExecutorInfos()),
                'active_jobs': len(sc.statusTracker().getActiveJobIds()),
                'active_stages': len(sc.statusTracker().getActiveStageIds()),
            }
            
            return metrics
            
        except Exception as e:
            self.logger.error("Failed to get Spark metrics", exception=e)
            return {}
    
    def cleanup_spark_session(self, spark: SparkSession):
        """Nettoie proprement une session Spark"""
        try:
            spark.catalog.clearCache()
            spark.stop()
            self.logger.info("Spark session cleaned up")
        except Exception as e:
            self.logger.error("Spark session cleanup failed", exception=e)