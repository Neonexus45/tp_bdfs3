from typing import Dict, Any
from .config_manager import ConfigManager


class SparkConfig:
    """Configuration spécialisée pour Spark avec optimisations"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self._base_config = self.config_manager.get_spark_config()
    
    def get_feeder_config(self) -> Dict[str, str]:
        """Configuration optimisée pour l'application Feeder"""
        config = self._base_config.copy()
        config.update({
            'spark.app.name': 'lakehouse-accidents-feeder',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.files.maxPartitionBytes': '134217728',
            'spark.sql.adaptive.advisoryPartitionSizeInBytes': '67108864',
            'spark.sql.files.openCostInBytes': '4194304',
            'spark.sql.adaptive.coalescePartitions.minPartitionNum': '1',
            'spark.sql.adaptive.coalescePartitions.initialPartitionNum': '200',
        })
        return config
    
    def get_preprocessor_config(self) -> Dict[str, str]:
        """Configuration optimisée pour l'application Preprocessor"""
        config = self._base_config.copy()
        config.update({
            'spark.app.name': 'lakehouse-accidents-preprocessor',
            'spark.sql.adaptive.skewJoin.enabled': 'true',
            'spark.sql.adaptive.localShuffleReader.enabled': 'true',
            'spark.sql.adaptive.skewJoin.skewedPartitionFactor': '5',
            'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes': '256MB',
            'spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin': '0.2',
            'spark.sql.cache.serializer': 'org.apache.spark.sql.execution.columnar.InMemoryTableScanExec',
        })
        return config
    
    def get_datamart_config(self) -> Dict[str, str]:
        """Configuration optimisée pour l'application Datamart"""
        config = self._base_config.copy()
        config.update({
            'spark.app.name': 'lakehouse-accidents-datamart',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.minPartitionNum': '1',
            'spark.sql.execution.arrow.maxRecordsPerBatch': '10000',
            'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB',
            'spark.sql.adaptive.coalescePartitions.parallelismFirst': 'false',
        })
        return config
    
    def get_ml_training_config(self) -> Dict[str, str]:
        """Configuration optimisée pour l'application ML Training"""
        config = self._base_config.copy()
        config.update({
            'spark.app.name': 'lakehouse-accidents-ml-training',
            'spark.executor.memory': '3g',
            'spark.driver.memory': '2g',
            'spark.sql.execution.arrow.pyspark.enabled': 'true',
            'spark.sql.execution.arrow.maxRecordsPerBatch': '10000',
            'spark.sql.execution.arrow.fallback.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.localShuffleReader.enabled': 'true',
        })
        return config
    
    def get_streaming_config(self) -> Dict[str, str]:
        """Configuration optimisée pour le streaming"""
        config = self._base_config.copy()
        config.update({
            'spark.app.name': 'lakehouse-accidents-streaming',
            'spark.sql.streaming.checkpointLocation': '/tmp/spark-streaming-checkpoint',
            'spark.sql.streaming.forceDeleteTempCheckpointLocation': 'true',
            'spark.sql.adaptive.enabled': 'false',
            'spark.sql.adaptive.coalescePartitions.enabled': 'false',
            'spark.streaming.backpressure.enabled': 'true',
            'spark.streaming.receiver.maxRate': '1000',
        })
        return config
    
    def get_config_for_app(self, app_name: str) -> Dict[str, str]:
        """Récupère la configuration pour une application spécifique"""
        config_methods = {
            'feeder': self.get_feeder_config,
            'preprocessor': self.get_preprocessor_config,
            'datamart': self.get_datamart_config,
            'ml_training': self.get_ml_training_config,
            'streaming': self.get_streaming_config,
        }
        
        method = config_methods.get(app_name.lower())
        if method:
            return method()
        else:
            return self._base_config
    
    def get_optimized_config(self, data_size_gb: float, num_cores: int = None) -> Dict[str, str]:
        """Configuration dynamique basée sur la taille des données"""
        config = self._base_config.copy()
        
        if data_size_gb < 1:
            config.update({
                'spark.sql.shuffle.partitions': '50',
                'spark.executor.memory': '1g',
                'spark.executor.cores': '1',
            })
        elif data_size_gb < 10:
            config.update({
                'spark.sql.shuffle.partitions': '200',
                'spark.executor.memory': '2g',
                'spark.executor.cores': '2',
            })
        elif data_size_gb < 100:
            config.update({
                'spark.sql.shuffle.partitions': '400',
                'spark.executor.memory': '4g',
                'spark.executor.cores': '3',
            })
        else:
            config.update({
                'spark.sql.shuffle.partitions': '800',
                'spark.executor.memory': '6g',
                'spark.executor.cores': '4',
            })
        
        if num_cores:
            config['spark.executor.cores'] = str(min(num_cores, 4))
        
        return config
    
    def get_memory_optimized_config(self) -> Dict[str, str]:
        """Configuration optimisée pour les opérations mémoire-intensives"""
        config = self._base_config.copy()
        config.update({
            'spark.executor.memory': '4g',
            'spark.driver.memory': '2g',
            'spark.executor.memoryFraction': '0.8',
            'spark.storage.memoryFraction': '0.5',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.minPartitionNum': '1',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.kryo.unsafe': 'true',
            'spark.kryoserializer.buffer.max': '1024m',
        })
        return config
    
    def get_io_optimized_config(self) -> Dict[str, str]:
        """Configuration optimisée pour les opérations I/O intensives"""
        config = self._base_config.copy()
        config.update({
            'spark.sql.files.maxPartitionBytes': '268435456',
            'spark.sql.files.openCostInBytes': '8388608',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.advisoryPartitionSizeInBytes': '134217728',
            'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': '2',
            'spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored': 'true',
            'spark.sql.parquet.compression.codec': 'snappy',
            'spark.sql.parquet.filterPushdown': 'true',
            'spark.sql.parquet.mergeSchema': 'false',
        })
        return config