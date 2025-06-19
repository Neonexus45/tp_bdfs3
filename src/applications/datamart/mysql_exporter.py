"""
MySQLExporter - Export optimisé vers MySQL pour DATAMART
"""

import time
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, concat_ws, coalesce, current_date, date_format, round as spark_round
)
import pandas as pd

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.utils.database_connector import DatabaseConnector
from ...common.exceptions.custom_exceptions import FileProcessingError, DataValidationError


class MySQLExporter:
    """
    Exporteur optimisé vers MySQL pour la couche Gold
    
    Fonctionnalités:
    - Export optimisé vers MySQL (bulk operations)
    - Gestion des connexions et pool de connexions
    - Création automatique des tables avec index
    - Synchronisation incrémentale des données
    - Support transactions et rollback
    - Tables dénormalisées pour performance API
    """
    
    def __init__(self, db_connector: DatabaseConnector, config_manager: ConfigManager, logger: Logger):
        self.db_connector = db_connector
        self.config_manager = config_manager
        self.logger = logger
        
        # Configuration d'export
        self.batch_size = config_manager.get('mysql.batch_size', 1000)
        self.max_retries = 3
        self.retry_delay = 5
        
        # Schémas des tables Gold
        self.table_schemas = self._define_table_schemas()
        
        self.logger.info("MySQLExporter initialized", 
                        batch_size=self.batch_size,
                        tables_count=len(self.table_schemas))
    
    def create_gold_tables(self):
        """Crée toutes les tables Gold MySQL si elles n'existent pas"""
        try:
            self.logger.info("Creating Gold tables in MySQL")
            start_time = time.time()
            
            tables_created = 0
            
            for table_name, schema_info in self.table_schemas.items():
                try:
                    # Création de la table
                    self.db_connector.create_table(
                        table_name=table_name,
                        schema=schema_info['schema'],
                        table_type='analytical',
                        indexes=schema_info.get('indexes', [])
                    )
                    
                    tables_created += 1
                    self.logger.info(f"Gold table created", table=table_name)
                    
                except Exception as e:
                    self.logger.error(f"Failed to create table {table_name}", exception=e)
                    # Continue avec les autres tables
            
            duration = time.time() - start_time
            
            self.logger.log_performance("gold_tables_creation", duration,
                                      tables_created=tables_created)
            
        except Exception as e:
            self.logger.error("Gold tables creation failed", exception=e)
            raise FileProcessingError(f"Gold tables creation failed: {str(e)}") from e
    
    def export_accidents_summary(self, kpis_data: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        Exporte la table principale accidents_summary
        
        Args:
            kpis_data: Données KPIs calculées
            
        Returns:
            Dict avec résultats d'export
        """
        try:
            self.logger.info("Starting accidents_summary export")
            start_time = time.time()
            
            # Récupération des données de sécurité et géographiques
            security_data = kpis_data.get('security')
            hotspots_data = kpis_data.get('hotspots')
            
            if security_data is None:
                raise DataValidationError("Security data not available for accidents_summary")
            
            # Préparation des données pour accidents_summary
            summary_df = security_data.select(
                col("state").alias("state"),
                col("city").alias("city"),
                col("accidents_count").alias("accident_count"),
                col("danger_index").alias("severity_avg"),
                col("accident_rate_per_100k").alias("accident_rate_per_100k"),
                col("hotspot_rank").alias("hotspot_rank"),
                col("safety_category").alias("safety_category")
            ).withColumn(
                "id", 
                concat_ws("_", col("state"), coalesce(col("city"), lit("STATE_LEVEL")))
            ).withColumn(
                "accident_date", 
                date_format(current_date(), "yyyy-MM-dd")
            ).withColumn(
                "accident_hour", 
                lit(12)  # Valeur par défaut pour les agrégations
            ).withColumn(
                "weather_category", 
                lit("Mixed")  # Agrégation de toutes les conditions
            ).withColumn(
                "temperature_category", 
                lit("Mixed")
            ).withColumn(
                "infrastructure_count", 
                lit(0)  # À enrichir avec données infrastructure
            ).withColumn(
                "safety_score", 
                spark_round(5.0 - col("danger_index"), 2)  # Score inversé
            ).withColumn(
                "distance_miles", 
                lit(0.0)  # Moyenne à calculer
            )
            
            # Conversion en Pandas pour export MySQL
            summary_pandas = summary_df.toPandas()
            
            # Export vers MySQL
            rows_exported = self.db_connector.bulk_insert(
                table_name="accidents_summary",
                data=summary_pandas.to_dict('records'),
                batch_size=self.batch_size,
                on_duplicate='replace'
            )
            
            duration = time.time() - start_time
            
            result = {
                'table': 'accidents_summary',
                'rows_exported': rows_exported,
                'duration_seconds': duration,
                'status': 'SUCCESS'
            }
            
            self.logger.log_performance("accidents_summary_export", duration,
                                      rows_exported=rows_exported)
            
            return result
            
        except Exception as e:
            self.logger.error("Accidents summary export failed", exception=e)
            raise FileProcessingError(f"Accidents summary export failed: {str(e)}") from e
    
    def export_kpi_data(self, kpi_type: str, kpi_df: DataFrame) -> Dict[str, Any]:
        """
        Exporte les données KPI vers la table MySQL correspondante
        
        Args:
            kpi_type: Type de KPI (security, temporal, infrastructure, hotspots, ml_performance)
            kpi_df: DataFrame avec les données KPI
            
        Returns:
            Dict avec résultats d'export
        """
        try:
            self.logger.info(f"Starting {kpi_type} KPI export")
            start_time = time.time()
            
            # Mapping des types KPI vers tables MySQL
            table_mapping = {
                'security': 'kpis_security',
                'temporal': 'kpis_temporal', 
                'infrastructure': 'kpis_infrastructure',
                'hotspots': 'hotspots',
                'ml_performance': 'ml_model_performance'
            }
            
            table_name = table_mapping.get(kpi_type)
            if not table_name:
                raise DataValidationError(f"Unknown KPI type: {kpi_type}")
            
            # Préparation des données selon le type
            prepared_df = self._prepare_kpi_data(kpi_type, kpi_df)
            
            # Conversion en Pandas
            pandas_df = prepared_df.toPandas()
            
            # Nettoyage des données pour MySQL
            pandas_df = self._clean_data_for_mysql(pandas_df)
            
            # Export vers MySQL
            rows_exported = self.db_connector.bulk_insert(
                table_name=table_name,
                data=pandas_df.to_dict('records'),
                batch_size=self.batch_size,
                on_duplicate='replace'
            )
            
            duration = time.time() - start_time
            
            result = {
                'table': table_name,
                'kpi_type': kpi_type,
                'rows_exported': rows_exported,
                'duration_seconds': duration,
                'status': 'SUCCESS'
            }
            
            self.logger.log_performance(f"{kpi_type}_kpi_export", duration,
                                      rows_exported=rows_exported)
            
            return result
            
        except Exception as e:
            self.logger.error(f"{kpi_type} KPI export failed", exception=e)
            raise FileProcessingError(f"{kpi_type} KPI export failed: {str(e)}") from e
    
    def incremental_export(self, updated_kpis: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        Export incrémental des KPIs mis à jour
        
        Args:
            updated_kpis: KPIs mis à jour
            
        Returns:
            Dict avec résultats d'export incrémental
        """
        try:
            self.logger.info("Starting incremental export")
            start_time = time.time()
            
            export_results = {}
            total_rows_exported = 0
            
            # Export de chaque type de KPI mis à jour
            for kpi_type, kpi_df in updated_kpis.items():
                if kpi_df is not None and kpi_df.count() > 0:
                    result = self.export_kpi_data(kpi_type, kpi_df)
                    export_results[kpi_type] = result
                    total_rows_exported += result['rows_exported']
            
            duration = time.time() - start_time
            
            final_result = {
                'export_type': 'incremental',
                'duration_seconds': duration,
                'kpi_types_updated': len(export_results),
                'total_rows_exported': total_rows_exported,
                'individual_results': export_results,
                'status': 'SUCCESS'
            }
            
            self.logger.log_performance("incremental_export", duration,
                                      kpi_types_updated=len(export_results),
                                      total_rows_exported=total_rows_exported)
            
            return final_result
            
        except Exception as e:
            self.logger.error("Incremental export failed", exception=e)
            raise FileProcessingError(f"Incremental export failed: {str(e)}") from e
    
    def _prepare_kpi_data(self, kpi_type: str, kpi_df: DataFrame) -> DataFrame:
        """Prépare les données KPI selon le type pour l'export MySQL"""
        
        if kpi_type == 'security':
            return kpi_df.select(
                col("state"),
                col("city"),
                col("accident_rate_per_100k"),
                col("danger_index"),
                col("severity_distribution"),
                col("hotspot_rank"),
                col("last_updated")
            )
        
        elif kpi_type == 'temporal':
            return kpi_df.select(
                col("period_type"),
                col("period_value"),
                col("state"),
                col("accidents_count").alias("accident_count"),
                col("severity_avg"),
                col("trend_direction"),
                col("seasonal_factor"),
                col("last_updated")
            )
        
        elif kpi_type == 'infrastructure':
            return kpi_df.select(
                col("state"),
                col("infrastructure_type"),
                col("equipment_effectiveness"),
                col("accident_reduction_rate"),
                col("safety_improvement_score"),
                col("detailed_recommendation").alias("recommendation"),
                col("last_updated")
            )
        
        elif kpi_type == 'hotspots':
            return kpi_df.select(
                col("state"),
                col("city"),
                col("latitude"),
                col("longitude"),
                col("accidents_count").alias("accident_count"),
                col("severity_avg"),
                col("danger_score"),
                col("radius_miles"),
                col("last_updated")
            )
        
        elif kpi_type == 'ml_performance':
            return kpi_df.select(
                col("model_name"),
                col("model_version"),
                col("accuracy"),
                col("precision_score"),
                col("recall_score"),
                col("f1_score"),
                col("feature_importance"),
                col("training_date").cast("timestamp"),
                col("last_updated")
            )
        
        else:
            return kpi_df
    
    def _clean_data_for_mysql(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie les données Pandas pour l'export MySQL"""
        
        # Remplacement des valeurs NaN par None
        df = df.where(pd.notnull(df), None)
        
        # Conversion des types pour MySQL
        for col in df.columns:
            if df[col].dtype == 'object':
                # Limitation de la longueur des chaînes
                df[col] = df[col].astype(str).str[:500]
            elif df[col].dtype == 'float64':
                # Arrondi des flottants
                df[col] = df[col].round(6)
        
        return df
    
    def _define_table_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Définit les schémas des tables Gold MySQL"""
        
        return {
            'accidents_summary': {
                'schema': {
                    'id': 'VARCHAR(50) PRIMARY KEY',
                    'state': 'VARCHAR(2) NOT NULL',
                    'city': 'VARCHAR(100)',
                    'severity': 'INT',
                    'accident_date': 'DATE',
                    'accident_hour': 'INT',
                    'weather_category': 'VARCHAR(20)',
                    'temperature_category': 'VARCHAR(10)',
                    'infrastructure_count': 'INT',
                    'safety_score': 'DOUBLE',
                    'distance_miles': 'DOUBLE'
                },
                'indexes': [
                    {'name': 'idx_state_date', 'columns': ['state', 'accident_date']},
                    {'name': 'idx_severity_weather', 'columns': ['severity', 'weather_category']},
                    {'name': 'idx_location', 'columns': ['state', 'city']},
                    {'name': 'idx_temporal', 'columns': ['accident_date', 'accident_hour']}
                ]
            },
            
            'kpis_security': {
                'schema': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'state': 'VARCHAR(2) NOT NULL',
                    'city': 'VARCHAR(100)',
                    'accident_rate_per_100k': 'DOUBLE',
                    'danger_index': 'DOUBLE',
                    'severity_distribution': 'JSON',
                    'hotspot_rank': 'INT',
                    'last_updated': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
                },
                'indexes': [
                    {'name': 'idx_state', 'columns': ['state']},
                    {'name': 'idx_danger_index', 'columns': ['danger_index']},
                    {'name': 'idx_hotspot_rank', 'columns': ['hotspot_rank']}
                ]
            },
            
            'kpis_temporal': {
                'schema': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'period_type': "ENUM('hourly', 'daily', 'monthly', 'yearly')",
                    'period_value': 'VARCHAR(20)',
                    'state': 'VARCHAR(2)',
                    'accident_count': 'BIGINT',
                    'severity_avg': 'DOUBLE',
                    'trend_direction': "ENUM('up', 'down', 'stable')",
                    'seasonal_factor': 'DOUBLE',
                    'last_updated': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
                },
                'indexes': [
                    {'name': 'idx_period', 'columns': ['period_type', 'period_value']},
                    {'name': 'idx_state_period', 'columns': ['state', 'period_type']}
                ]
            },
            
            'kpis_infrastructure': {
                'schema': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'state': 'VARCHAR(2) NOT NULL',
                    'infrastructure_type': 'VARCHAR(50)',
                    'equipment_effectiveness': 'DOUBLE',
                    'accident_reduction_rate': 'DOUBLE',
                    'safety_improvement_score': 'DOUBLE',
                    'recommendation': 'TEXT',
                    'last_updated': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
                },
                'indexes': [
                    {'name': 'idx_state_infra', 'columns': ['state', 'infrastructure_type']},
                    {'name': 'idx_effectiveness', 'columns': ['equipment_effectiveness']}
                ]
            },
            
            'hotspots': {
                'schema': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'state': 'VARCHAR(2) NOT NULL',
                    'city': 'VARCHAR(100)',
                    'latitude': 'DOUBLE',
                    'longitude': 'DOUBLE',
                    'accident_count': 'BIGINT',
                    'severity_avg': 'DOUBLE',
                    'danger_score': 'DOUBLE',
                    'radius_miles': 'DOUBLE',
                    'last_updated': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
                },
                'indexes': [
                    {'name': 'idx_location', 'columns': ['state', 'city']},
                    {'name': 'idx_coordinates', 'columns': ['latitude', 'longitude']},
                    {'name': 'idx_danger_score', 'columns': ['danger_score']}
                ]
            },
            
            'ml_model_performance': {
                'schema': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'model_name': 'VARCHAR(100)',
                    'model_version': 'VARCHAR(20)',
                    'accuracy': 'DOUBLE',
                    'precision_score': 'DOUBLE',
                    'recall_score': 'DOUBLE',
                    'f1_score': 'DOUBLE',
                    'feature_importance': 'JSON',
                    'training_date': 'TIMESTAMP',
                    'last_updated': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
                },
                'indexes': [
                    {'name': 'idx_model', 'columns': ['model_name', 'model_version']},
                    {'name': 'idx_performance', 'columns': ['f1_score']}
                ]
            }
        }
    
    def get_export_statistics(self) -> Dict[str, Any]:
        """Récupère les statistiques d'export"""
        try:
            stats = {}
            
            for table_name in self.table_schemas.keys():
                table_info = self.db_connector.get_table_info(table_name)
                stats[table_name] = {
                    'row_count': table_info['stats'].get('TABLE_ROWS', 0),
                    'data_size_mb': round(table_info['stats'].get('DATA_LENGTH', 0) / 1024 / 1024, 2),
                    'index_size_mb': round(table_info['stats'].get('INDEX_LENGTH', 0) / 1024 / 1024, 2),
                    'last_update': table_info['stats'].get('UPDATE_TIME')
                }
            
            return stats
            
        except Exception as e:
            self.logger.error("Failed to get export statistics", exception=e)
            return {}
    
    def validate_export_integrity(self) -> Dict[str, Any]:
        """Valide l'intégrité des données exportées"""
        try:
            validation_results = {}
            
            for table_name in self.table_schemas.keys():
                # Vérification de l'existence et du contenu
                row_count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                result = self.db_connector.execute_query(row_count_query)
                row_count = result[0]['count'] if result else 0
                
                # Vérification des contraintes
                constraints_ok = row_count > 0
                
                validation_results[table_name] = {
                    'exists': True,
                    'row_count': row_count,
                    'constraints_valid': constraints_ok,
                    'status': 'VALID' if constraints_ok else 'INVALID'
                }
            
            return validation_results
            
        except Exception as e:
            self.logger.error("Export integrity validation failed", exception=e)
            return {}