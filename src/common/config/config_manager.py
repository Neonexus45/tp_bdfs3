import os
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
import yaml


class ConfigManager:
    """Gestionnaire centralisé de configuration avec pattern Singleton"""
    
    _instance: Optional['ConfigManager'] = None
    _config: Optional[Dict[str, Any]] = None
    
    def __new__(cls) -> 'ConfigManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Charge la configuration depuis multiples sources"""
        load_dotenv()
        
        config_file = Path("config/base.yaml")
        yaml_config = {}
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                yaml_config = yaml.safe_load(f) or {}
        
        self._config = {
            **yaml_config,
            **self._get_env_config()
        }
        
        self._validate_config()
    
    def _get_env_config(self) -> Dict[str, Any]:
        """Extrait la configuration depuis les variables d'environnement"""
        return {
            'mysql': {
                'host': os.getenv('MYSQL_HOST', 'localhost'),
                'port': int(os.getenv('MYSQL_PORT', '3306')),
                'database': os.getenv('MYSQL_DATABASE', 'accidents_lakehouse'),
                'user': os.getenv('MYSQL_USER', 'tatane'),
                'password': os.getenv('MYSQL_PASSWORD', 'tatane'),
                'pool_size': int(os.getenv('MYSQL_POOL_SIZE', '10')),
                'max_overflow': int(os.getenv('MYSQL_MAX_OVERFLOW', '20')),
                'pool_timeout': int(os.getenv('MYSQL_POOL_TIMEOUT', '30')),
                'pool_recycle': int(os.getenv('MYSQL_POOL_RECYCLE', '3600')),
            },
            
            'hdfs': {
                'namenode': os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000'),
                'base_path': os.getenv('HDFS_BASE_PATH', '/lakehouse/accidents'),
                'bronze_path': os.getenv('HDFS_BRONZE_PATH', '/lakehouse/accidents/bronze'),
                'silver_path': os.getenv('HDFS_SILVER_PATH', '/lakehouse/accidents/silver'),
                'gold_path': os.getenv('HDFS_GOLD_PATH', '/lakehouse/accidents/gold'),
                'replication_factor': int(os.getenv('HDFS_REPLICATION_FACTOR', '3')),
                'block_size': int(os.getenv('HDFS_BLOCK_SIZE', '134217728')),
            },
            
            'spark': {
                'master': os.getenv('SPARK_MASTER', 'yarn'),
                'app_name': os.getenv('SPARK_APP_NAME', 'lakehouse-accidents-us'),
                'deploy_mode': os.getenv('SPARK_DEPLOY_MODE', 'client'),
                'driver_memory': os.getenv('SPARK_DRIVER_MEMORY', '1g'),
                'driver_max_result_size': os.getenv('SPARK_DRIVER_MAX_RESULT_SIZE', '512m'),
                'executor_memory': os.getenv('SPARK_EXECUTOR_MEMORY', '2g'),
                'executor_cores': int(os.getenv('SPARK_EXECUTOR_CORES', '2')),
                'executor_instances': int(os.getenv('SPARK_EXECUTOR_INSTANCES', '4')),
                'dynamic_allocation_enabled': os.getenv('SPARK_DYNAMIC_ALLOCATION_ENABLED', 'true').lower() == 'true',
                'dynamic_allocation_min_executors': int(os.getenv('SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS', '1')),
                'dynamic_allocation_max_executors': int(os.getenv('SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS', '4')),
                'sql_shuffle_partitions': int(os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '200')),
                'serializer': os.getenv('SPARK_SERIALIZER', 'org.apache.spark.serializer.KryoSerializer'),
                'sql_adaptive_enabled': os.getenv('SPARK_SQL_ADAPTIVE_ENABLED', 'true').lower() == 'true',
                'sql_adaptive_coalesce_partitions_enabled': os.getenv('SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED', 'true').lower() == 'true',
                'sql_adaptive_skew_join_enabled': os.getenv('SPARK_SQL_ADAPTIVE_SKEW_JOIN_ENABLED', 'true').lower() == 'true',
                'sql_execution_arrow_pyspark_enabled': os.getenv('SPARK_SQL_EXECUTION_ARROW_PYSPARK_ENABLED', 'true').lower() == 'true',
            },
            
            'hive': {
                'metastore_uri': os.getenv('HIVE_METASTORE_URI', 'thrift://localhost:9083'),
                'database': os.getenv('HIVE_DATABASE', 'accidents_warehouse'),
                'warehouse_dir': os.getenv('HIVE_WAREHOUSE_DIR', '/user/hive/warehouse'),
                'tables': {
                    'accidents_clean': os.getenv('HIVE_TABLE_ACCIDENTS_CLEAN', 'accidents_clean'),
                    'weather_aggregated': os.getenv('HIVE_TABLE_WEATHER_AGGREGATED', 'weather_aggregated'),
                    'infrastructure_features': os.getenv('HIVE_TABLE_INFRASTRUCTURE_FEATURES', 'infrastructure_features'),
                    'temporal_patterns': os.getenv('HIVE_TABLE_TEMPORAL_PATTERNS', 'temporal_patterns'),
                    'geographic_hotspots': os.getenv('HIVE_TABLE_GEOGRAPHIC_HOTSPOTS', 'geographic_hotspots'),
                },
            },
            
            'api': {
                'host': os.getenv('API_HOST', '0.0.0.0'),
                'port': int(os.getenv('API_PORT', '8000')),
                'title': os.getenv('API_TITLE', 'Lakehouse Accidents US API'),
                'description': os.getenv('API_DESCRIPTION', 'API d\'analyse des accidents de la route aux États-Unis'),
                'version': os.getenv('API_VERSION', '1.0.0'),
                'secret_key': os.getenv('API_SECRET_KEY', 'change-me-in-production'),
                'access_token_expire_minutes': int(os.getenv('API_ACCESS_TOKEN_EXPIRE_MINUTES', '30')),
                'default_page_size': int(os.getenv('API_DEFAULT_PAGE_SIZE', '100')),
                'max_page_size': int(os.getenv('API_MAX_PAGE_SIZE', '1000')),
                'cors': {
                    'origins': eval(os.getenv('API_CORS_ORIGINS', '["http://localhost:3000", "http://localhost:8080"]')),
                    'allow_credentials': os.getenv('API_CORS_ALLOW_CREDENTIALS', 'true').lower() == 'true',
                    'allow_methods': eval(os.getenv('API_CORS_ALLOW_METHODS', '["GET", "POST", "PUT", "DELETE"]')),
                    'allow_headers': eval(os.getenv('API_CORS_ALLOW_HEADERS', '["*"]')),
                },
            },
            
            'logging': {
                'level': os.getenv('LOG_LEVEL', 'INFO'),
                'format': os.getenv('LOG_FORMAT', 'json'),
                'file_path': os.getenv('LOG_FILE_PATH', '/var/log/lakehouse-accidents'),
                'file_max_size': os.getenv('LOG_FILE_MAX_SIZE', '100MB'),
                'file_backup_count': int(os.getenv('LOG_FILE_BACKUP_COUNT', '5')),
                'rotation': os.getenv('LOG_ROTATION', 'daily'),
                'app_levels': {
                    'feeder': os.getenv('LOG_FEEDER_LEVEL', 'INFO'),
                    'preprocessor': os.getenv('LOG_PREPROCESSOR_LEVEL', 'INFO'),
                    'datamart': os.getenv('LOG_DATAMART_LEVEL', 'INFO'),
                    'ml_training': os.getenv('LOG_ML_TRAINING_LEVEL', 'INFO'),
                    'api': os.getenv('LOG_API_LEVEL', 'INFO'),
                },
            },
            
            'mlflow': {
                'tracking_uri': os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000'),
                'experiment_name': os.getenv('MLFLOW_EXPERIMENT_NAME', 'accidents-severity-prediction'),
                'artifact_root': os.getenv('MLFLOW_ARTIFACT_ROOT', '/mlflow/artifacts'),
                'default_artifact_root': os.getenv('MLFLOW_DEFAULT_ARTIFACT_ROOT', '/mlflow/artifacts'),
            },
            
            'monitoring': {
                'enabled': os.getenv('MONITORING_ENABLED', 'true').lower() == 'true',
                'metrics_port': int(os.getenv('METRICS_PORT', '9090')),
                'health_check_interval': int(os.getenv('HEALTH_CHECK_INTERVAL', '30')),
                'performance_monitoring_enabled': os.getenv('PERFORMANCE_MONITORING_ENABLED', 'true').lower() == 'true',
                'alert_thresholds': {
                    'memory': int(os.getenv('ALERT_MEMORY_THRESHOLD', '80')),
                    'cpu': int(os.getenv('ALERT_CPU_THRESHOLD', '85')),
                    'disk': int(os.getenv('ALERT_DISK_THRESHOLD', '90')),
                    'error_rate': int(os.getenv('ALERT_ERROR_RATE_THRESHOLD', '5')),
                },
            },
            
            'data': {
                'source_path': os.getenv('DATA_SOURCE_PATH', 'data/raw/US_Accidents_March23.csv'),
                'expected_columns': int(os.getenv('DATA_EXPECTED_COLUMNS', '47')),
                'expected_rows': int(os.getenv('DATA_EXPECTED_ROWS', '7700000')),
                'partition_columns': eval(os.getenv('PARTITION_COLUMNS', '["date", "state"]')),
                'bucketing_columns': eval(os.getenv('BUCKETING_COLUMNS', '["state", "county"]')),
                'bucketing_num_buckets': int(os.getenv('BUCKETING_NUM_BUCKETS', '50')),
                'compression_codec': os.getenv('COMPRESSION_CODEC', 'snappy'),
                'parquet_compression': os.getenv('PARQUET_COMPRESSION', 'snappy'),
            },
            
            'generator': {
                'enabled': os.getenv('GENERATOR_ENABLED', 'true').lower() == 'true',
                'interval_seconds': int(os.getenv('GENERATOR_INTERVAL_SECONDS', '60')),
                'batch_size': int(os.getenv('GENERATOR_BATCH_SIZE', '100')),
                'simulation_speed': float(os.getenv('GENERATOR_SIMULATION_SPEED', '1.0')),
            },
            
            'security': {
                'enabled': os.getenv('SECURITY_ENABLED', 'true').lower() == 'true',
                'jwt_secret_key': os.getenv('JWT_SECRET_KEY', 'your-jwt-secret-key'),
                'jwt_algorithm': os.getenv('JWT_ALGORITHM', 'HS256'),
                'jwt_access_token_expire_minutes': int(os.getenv('JWT_ACCESS_TOKEN_EXPIRE_MINUTES', '30')),
            },
            
            'environment': os.getenv('ENVIRONMENT', 'development'),
            'debug': os.getenv('DEBUG', 'true').lower() == 'true',
            'testing': os.getenv('TESTING', 'false').lower() == 'true',
        }
    
    def _validate_config(self) -> None:
        """Valide la cohérence de la configuration"""
        required_keys = [
            'mysql.host', 'mysql.user', 'mysql.password',
            'hdfs.namenode', 'spark.master',
            'api.secret_key'
        ]
        
        for key in required_keys:
            if not self._get_nested_value(key):
                raise ValueError(f"Configuration manquante: {key}")
    
    def _get_nested_value(self, key: str) -> Any:
        """Récupère une valeur imbriquée avec notation pointée"""
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None
        return value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Récupère une valeur de configuration"""
        return self._get_nested_value(key) or default
    
    def get_mysql_url(self) -> str:
        """Construit l'URL de connexion MySQL"""
        mysql_config = self._config['mysql']
        return (f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
                f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
    
    def get_hdfs_path(self, layer: str = 'bronze') -> str:
        """Récupère le chemin HDFS pour une couche donnée"""
        hdfs_config = self._config['hdfs']
        return hdfs_config.get(f'{layer}_path', hdfs_config['base_path'])
    
    def get_spark_config(self) -> Dict[str, str]:
        """Récupère la configuration Spark complète"""
        spark_config = self._config['spark']
        return {
            'spark.master': spark_config['master'],
            'spark.app.name': spark_config['app_name'],
            'spark.submit.deployMode': spark_config['deploy_mode'],
            'spark.driver.memory': spark_config['driver_memory'],
            'spark.driver.maxResultSize': spark_config['driver_max_result_size'],
            'spark.executor.memory': spark_config['executor_memory'],
            'spark.executor.cores': str(spark_config['executor_cores']),
            'spark.executor.instances': str(spark_config['executor_instances']),
            'spark.dynamicAllocation.enabled': str(spark_config['dynamic_allocation_enabled']).lower(),
            'spark.dynamicAllocation.minExecutors': str(spark_config['dynamic_allocation_min_executors']),
            'spark.dynamicAllocation.maxExecutors': str(spark_config['dynamic_allocation_max_executors']),
            'spark.sql.shuffle.partitions': str(spark_config['sql_shuffle_partitions']),
            'spark.serializer': spark_config['serializer'],
            'spark.sql.adaptive.enabled': str(spark_config['sql_adaptive_enabled']).lower(),
            'spark.sql.adaptive.coalescePartitions.enabled': str(spark_config['sql_adaptive_coalesce_partitions_enabled']).lower(),
            'spark.sql.adaptive.skewJoin.enabled': str(spark_config['sql_adaptive_skew_join_enabled']).lower(),
            'spark.sql.execution.arrow.pyspark.enabled': str(spark_config['sql_execution_arrow_pyspark_enabled']).lower(),
        }
    
    def get_hive_table_name(self, table_key: str) -> str:
        """Récupère le nom d'une table Hive"""
        return self.get(f'hive.tables.{table_key}', table_key)
    
    def is_development(self) -> bool:
        """Vérifie si l'environnement est en développement"""
        return self.get('environment') == 'development'
    
    def is_production(self) -> bool:
        """Vérifie si l'environnement est en production"""
        return self.get('environment') == 'production'