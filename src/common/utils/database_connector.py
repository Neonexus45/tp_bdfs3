from typing import Dict, Any, List, Optional, Union
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import pandas as pd
from ..config.config_manager import ConfigManager
from ..config.database_config import DatabaseConfig
from .logger import Logger


class DatabaseConnector:
    """Connecteur MySQL réutilisable avec gestion des connexions"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.db_config = DatabaseConfig(self.config_manager)
        self.logger = Logger("database_connector", self.config_manager)
        self._engine = None
        self._session_factory = None
    
    @property
    def engine(self):
        """Propriété lazy pour le moteur SQLAlchemy"""
        if self._engine is None:
            self._engine = self.db_config.create_mysql_engine()
            self.logger.info("Database engine created", database="mysql")
        return self._engine
    
    @property
    def session_factory(self):
        """Factory pour créer des sessions SQLAlchemy"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory
    
    @contextmanager
    def get_session(self):
        """Context manager pour les sessions SQLAlchemy"""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error("Database session error", exception=e)
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_connection(self):
        """Context manager pour les connexions directes"""
        connection = self.engine.connect()
        try:
            yield connection
        except Exception as e:
            self.logger.error("Database connection error", exception=e)
            raise
        finally:
            connection.close()
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Exécute une requête SELECT et retourne les résultats"""
        start_time = pd.Timestamp.now()
        
        try:
            with self.get_connection() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                rows = [dict(row._mapping) for row in result]
                
                duration_ms = (pd.Timestamp.now() - start_time).total_seconds() * 1000
                self.logger.log_database_operation(
                    operation="SELECT",
                    table="multiple",
                    rows_affected=len(rows),
                    duration_ms=duration_ms
                )
                
                return rows
                
        except Exception as e:
            self.logger.error("Query execution failed", exception=e, query=query)
            raise
    
    def execute_non_query(self, query: str, params: Dict[str, Any] = None) -> int:
        """Exécute une requête INSERT/UPDATE/DELETE"""
        start_time = pd.Timestamp.now()
        
        try:
            with self.get_connection() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                rows_affected = result.rowcount
                conn.commit()
                
                duration_ms = (pd.Timestamp.now() - start_time).total_seconds() * 1000
                self.logger.log_database_operation(
                    operation="MODIFY",
                    table="multiple",
                    rows_affected=rows_affected,
                    duration_ms=duration_ms
                )
                
                return rows_affected
                
        except Exception as e:
            self.logger.error("Non-query execution failed", exception=e, query=query)
            raise
    
    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]], 
                   batch_size: int = 1000, on_duplicate: str = 'ignore') -> int:
        """Insertion en masse optimisée"""
        if not data:
            return 0
        
        start_time = pd.Timestamp.now()
        total_inserted = 0
        
        try:
            df = pd.DataFrame(data)
            bulk_config = self.db_config.get_bulk_insert_config()
            
            if on_duplicate == 'replace':
                if_exists = 'replace'
            elif on_duplicate == 'update':
                if_exists = 'append'
            else:
                if_exists = 'append'
            
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=batch_size,
                method='multi'
            )
            
            total_inserted = len(data)
            
            duration_ms = (pd.Timestamp.now() - start_time).total_seconds() * 1000
            self.logger.log_database_operation(
                operation="BULK_INSERT",
                table=table_name,
                rows_affected=total_inserted,
                duration_ms=duration_ms
            )
            
            return total_inserted
            
        except Exception as e:
            self.logger.error("Bulk insert failed", exception=e, table=table_name)
            raise
    
    def create_table(self, table_name: str, schema: Dict[str, str], 
                    table_type: str = 'transactional', indexes: List[Dict[str, Any]] = None):
        """Crée une table avec le schéma spécifié"""
        try:
            columns = []
            for col_name, col_type in schema.items():
                columns.append(f"`{col_name}` {col_type}")
            
            table_options = self.db_config.get_table_creation_options(table_type)
            options_str = ' '.join([f"{k}={v}" for k, v in table_options.items()])
            
            create_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                {', '.join(columns)}
            ) {options_str}
            """
            
            self.execute_non_query(create_query)
            self.logger.info("Table created", table=table_name, table_type=table_type)
            
            if indexes:
                self.create_indexes(table_name, indexes)
                
        except Exception as e:
            self.logger.error("Table creation failed", exception=e, table=table_name)
            raise
    
    def create_indexes(self, table_name: str, indexes: List[Dict[str, Any]]):
        """Crée des index sur une table"""
        for index_def in indexes:
            try:
                index_name = index_def.get('name', f"idx_{table_name}_{index_def['columns'][0]}")
                columns = index_def['columns']
                index_type = index_def.get('type', 'BTREE')
                unique = 'UNIQUE' if index_def.get('unique', False) else ''
                
                create_index_query = f"""
                CREATE {unique} INDEX `{index_name}` 
                ON `{table_name}` ({', '.join([f"`{col}`" for col in columns])})
                USING {index_type}
                """
                
                self.execute_non_query(create_index_query)
                self.logger.info("Index created", table=table_name, index=index_name)
                
            except Exception as e:
                self.logger.error("Index creation failed", exception=e, 
                                table=table_name, index=index_def)
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Récupère les informations d'une table"""
        try:
            info_query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMN_KEY,
                EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """
            
            columns = self.execute_query(info_query)
            
            stats_query = f"""
            SELECT 
                TABLE_ROWS,
                DATA_LENGTH,
                INDEX_LENGTH,
                CREATE_TIME,
                UPDATE_TIME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}'
            """
            
            stats = self.execute_query(stats_query)
            
            return {
                'columns': columns,
                'stats': stats[0] if stats else {},
                'table_name': table_name
            }
            
        except Exception as e:
            self.logger.error("Failed to get table info", exception=e, table=table_name)
            raise
    
    def optimize_table(self, table_name: str):
        """Optimise une table MySQL"""
        try:
            self.execute_non_query(f"OPTIMIZE TABLE `{table_name}`")
            self.logger.info("Table optimized", table=table_name)
        except Exception as e:
            self.logger.error("Table optimization failed", exception=e, table=table_name)
            raise
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Récupère les statistiques de connexion"""
        try:
            stats_query = """
            SHOW STATUS WHERE Variable_name IN (
                'Connections', 'Max_used_connections', 'Threads_connected',
                'Threads_running', 'Uptime', 'Questions'
            )
            """
            
            stats_rows = self.execute_query(stats_query)
            stats = {row['Variable_name']: row['Value'] for row in stats_rows}
            
            return stats
            
        except Exception as e:
            self.logger.error("Failed to get connection stats", exception=e)
            return {}
    
    def test_connection(self) -> bool:
        """Test la connexion à la base de données"""
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Database connection test successful")
            return True
        except Exception as e:
            self.logger.error("Database connection test failed", exception=e)
            return False
    
    def close(self):
        """Ferme toutes les connexions"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            self.logger.info("Database connections closed")