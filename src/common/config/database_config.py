from typing import Dict, Any, Optional
from sqlalchemy import create_engine, Engine
from sqlalchemy.pool import QueuePool
from .config_manager import ConfigManager


class DatabaseConfig:
    """Configuration spécialisée pour les bases de données"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self._mysql_config = self.config_manager.get('mysql')
        self._hive_config = self.config_manager.get('hive')
        self._engine: Optional[Engine] = None
    
    def create_mysql_engine(self, **kwargs) -> Engine:
        """Crée un moteur SQLAlchemy pour MySQL avec pool de connexions"""
        if self._engine is None:
            url = self.config_manager.get_mysql_url()
            
            engine_config = {
                'poolclass': QueuePool,
                'pool_size': self._mysql_config['pool_size'],
                'max_overflow': self._mysql_config['max_overflow'],
                'pool_timeout': self._mysql_config['pool_timeout'],
                'pool_recycle': self._mysql_config['pool_recycle'],
                'echo': self.config_manager.get('debug', False),
                'pool_pre_ping': True,
                'pool_reset_on_return': 'commit',
            }
            
            engine_config.update(kwargs)
            self._engine = create_engine(url, **engine_config)
        
        return self._engine
    
    def get_mysql_connection_params(self) -> Dict[str, Any]:
        """Récupère les paramètres de connexion MySQL"""
        return {
            'host': self._mysql_config['host'],
            'port': self._mysql_config['port'],
            'database': self._mysql_config['database'],
            'user': self._mysql_config['user'],
            'password': self._mysql_config['password'],
            'charset': 'utf8mb4',
            'autocommit': False,
            'connect_timeout': 30,
            'read_timeout': 30,
            'write_timeout': 30,
        }
    
    def get_hive_connection_params(self) -> Dict[str, Any]:
        """Récupère les paramètres de connexion Hive"""
        return {
            'host': self._extract_host_from_uri(self._hive_config['metastore_uri']),
            'port': self._extract_port_from_uri(self._hive_config['metastore_uri']),
            'database': self._hive_config['database'],
            'auth': 'NOSASL',
            'configuration': {
                'hive.exec.dynamic.partition': 'true',
                'hive.exec.dynamic.partition.mode': 'nonstrict',
                'hive.exec.max.dynamic.partitions': '1000',
                'hive.exec.max.dynamic.partitions.pernode': '100',
            }
        }
    
    def get_spark_hive_config(self) -> Dict[str, str]:
        """Configuration Spark pour l'intégration Hive"""
        return {
            'spark.sql.catalogImplementation': 'hive',
            'spark.sql.warehouse.dir': self._hive_config['warehouse_dir'],
            'hive.metastore.uris': self._hive_config['metastore_uri'],
            'spark.sql.hive.metastore.version': '3.1.2',
            'spark.sql.hive.metastore.jars': 'builtin',
            'spark.sql.hive.convertMetastoreParquet': 'true',
            'spark.sql.hive.convertMetastoreOrc': 'true',
            'spark.sql.parquet.writeLegacyFormat': 'false',
            'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED',
            'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
        }
    
    def get_mysql_optimization_config(self) -> Dict[str, Any]:
        """Configuration d'optimisation MySQL"""
        return {
            'isolation_level': 'READ_COMMITTED',
            'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO',
            'innodb_buffer_pool_size': '1G',
            'innodb_log_file_size': '256M',
            'innodb_flush_log_at_trx_commit': 2,
            'innodb_flush_method': 'O_DIRECT',
            'query_cache_type': 'OFF',
            'tmp_table_size': '64M',
            'max_heap_table_size': '64M',
            'bulk_insert_buffer_size': '8M',
        }
    
    def get_connection_string(self, db_type: str = 'mysql') -> str:
        """Génère une chaîne de connexion pour différents types de DB"""
        if db_type.lower() == 'mysql':
            return self.config_manager.get_mysql_url()
        elif db_type.lower() == 'hive':
            hive_params = self.get_hive_connection_params()
            return f"hive://{hive_params['host']}:{hive_params['port']}/{hive_params['database']}"
        else:
            raise ValueError(f"Type de base de données non supporté: {db_type}")
    
    def get_table_creation_options(self, table_type: str = 'transactional') -> Dict[str, str]:
        """Options de création de tables optimisées"""
        if table_type == 'transactional':
            return {
                'ENGINE': 'InnoDB',
                'DEFAULT CHARSET': 'utf8mb4',
                'COLLATE': 'utf8mb4_unicode_ci',
                'ROW_FORMAT': 'DYNAMIC',
                'KEY_BLOCK_SIZE': '8',
            }
        elif table_type == 'analytical':
            return {
                'ENGINE': 'InnoDB',
                'DEFAULT CHARSET': 'utf8mb4',
                'COLLATE': 'utf8mb4_unicode_ci',
                'ROW_FORMAT': 'COMPRESSED',
                'KEY_BLOCK_SIZE': '4',
                'PARTITION BY RANGE': 'YEAR(created_date)',
            }
        elif table_type == 'archive':
            return {
                'ENGINE': 'ARCHIVE',
                'DEFAULT CHARSET': 'utf8mb4',
                'COLLATE': 'utf8mb4_unicode_ci',
            }
        else:
            return {
                'ENGINE': 'InnoDB',
                'DEFAULT CHARSET': 'utf8mb4',
                'COLLATE': 'utf8mb4_unicode_ci',
            }
    
    def get_index_creation_strategy(self, table_size: str = 'medium') -> Dict[str, Any]:
        """Stratégie de création d'index basée sur la taille de table"""
        strategies = {
            'small': {
                'index_type': 'BTREE',
                'key_block_size': None,
                'algorithm': 'INPLACE',
                'lock': 'NONE',
            },
            'medium': {
                'index_type': 'BTREE',
                'key_block_size': 8,
                'algorithm': 'INPLACE',
                'lock': 'SHARED',
            },
            'large': {
                'index_type': 'BTREE',
                'key_block_size': 4,
                'algorithm': 'COPY',
                'lock': 'EXCLUSIVE',
            }
        }
        return strategies.get(table_size, strategies['medium'])
    
    def get_bulk_insert_config(self) -> Dict[str, Any]:
        """Configuration pour les insertions en masse"""
        return {
            'batch_size': 1000,
            'chunk_size': 10000,
            'method': 'multi',
            'if_exists': 'append',
            'index': False,
            'chunksize': 1000,
            'method_kwargs': {
                'ignore_index': True,
                'on_duplicate_key_update': True,
            }
        }
    
    def _extract_host_from_uri(self, uri: str) -> str:
        """Extrait l'host d'une URI"""
        if '://' in uri:
            uri = uri.split('://')[1]
        if ':' in uri:
            return uri.split(':')[0]
        return uri
    
    def _extract_port_from_uri(self, uri: str) -> int:
        """Extrait le port d'une URI"""
        if '://' in uri:
            uri = uri.split('://')[1]
        if ':' in uri:
            return int(uri.split(':')[1])
        return 9083
    
    def close_engine(self):
        """Ferme le moteur de base de données"""
        if self._engine:
            self._engine.dispose()
            self._engine = None