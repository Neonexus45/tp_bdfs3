"""
Application principale DATAMART pour la création des tables analytiques optimisées et l'export vers MySQL
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from ...common.config.config_manager import ConfigManager
from ...common.config.spark_config import SparkConfig
from ...common.utils.logger import Logger
from ...common.utils.spark_utils import SparkUtils
from ...common.utils.database_connector import DatabaseConnector
from ...common.exceptions.custom_exceptions import (
    LakehouseException, SparkJobError, FileProcessingError, DataValidationError
)

from .business_aggregator import BusinessAggregator
from .kpi_calculator import KPICalculator
from .mysql_exporter import MySQLExporter
from .table_optimizer import TableOptimizer
from .analytics_engine import AnalyticsEngine


class DatamartApp:
    """
    Application principale DATAMART pour la couche Gold de l'architecture Medallion
    
    Fonctionnalités:
    - Lecture données Silver (Hive tables: accidents_clean, weather_aggregated, infrastructure_features)
    - Agrégations business multi-dimensionnelles (temps, géo, météo, infrastructure)
    - Calcul des KPIs business (sécurité, temporels, infrastructure, performance ML)
    - Export optimisé vers MySQL avec indexation pour FastAPI
    - Tables dénormalisées pour performance queries API
    - Optimisations Spark (broadcast joins, cache, coalesce, partitioning)
    - Logging détaillé des métriques analytics (JSON structuré)
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise l'application DATAMART"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("datamart", self.config_manager)
        self.spark_utils = SparkUtils(self.config_manager)
        self.db_connector = DatabaseConnector(self.config_manager)
        
        # Configuration spécifique
        self.silver_path = self.config_manager.get_hdfs_path('silver')
        self.gold_path = self.config_manager.get_hdfs_path('gold')
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Composants spécialisés
        self.business_aggregator = None
        self.kpi_calculator = None
        self.mysql_exporter = None
        self.table_optimizer = None
        self.analytics_engine = None
        
        # Métriques de performance
        self.start_time = None
        self.processing_metrics = {}
        
        self.logger.info("DatamartApp initialized", 
                        silver_path=self.silver_path,
                        gold_path=self.gold_path)
    
    def run(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Exécute le pipeline DATAMART complet
        
        Args:
            force_refresh: Force la recalcul de toutes les agrégations
            
        Returns:
            Dict contenant les métriques d'exécution
        """
        self.start_time = time.time()
        
        try:
            self.logger.info("Starting DATAMART pipeline", force_refresh=force_refresh)
            
            # 1. Initialisation des composants
            self._initialize_components()
            
            # 2. Validation des données Silver
            self._validate_silver_data()
            
            # 3. Agrégations business multi-dimensionnelles
            aggregated_data = self._perform_business_aggregations()
            
            # 4. Calcul des KPIs
            kpis_data = self._calculate_kpis(aggregated_data)
            
            # 5. Export vers MySQL
            export_results = self._export_to_mysql(kpis_data)
            
            # 6. Optimisation des tables MySQL
            self._optimize_mysql_tables()
            
            # 7. Génération des métriques finales
            final_metrics = self._generate_final_metrics(export_results)
            
            self.logger.info("DATAMART pipeline completed successfully", **final_metrics)
            return final_metrics
            
        except Exception as e:
            self.logger.error("DATAMART pipeline failed", exception=e)
            raise LakehouseException(f"DATAMART pipeline failed: {str(e)}") from e
        
        finally:
            self._cleanup_resources()
    
    def _initialize_components(self):
        """Initialise tous les composants spécialisés"""
        try:
            spark = self.spark_utils.get_spark_session("datamart-analytics")
            
            self.business_aggregator = BusinessAggregator(
                spark, self.config_manager, self.logger
            )
            
            self.kpi_calculator = KPICalculator(
                spark, self.config_manager, self.logger
            )
            
            self.mysql_exporter = MySQLExporter(
                self.db_connector, self.config_manager, self.logger
            )
            
            self.table_optimizer = TableOptimizer(
                self.db_connector, self.config_manager, self.logger
            )
            
            self.analytics_engine = AnalyticsEngine(
                spark, self.config_manager, self.logger
            )
            
            self.logger.info("All DATAMART components initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize DATAMART components", exception=e)
            raise
    
    def _validate_silver_data(self):
        """Valide la disponibilité et qualité des données Silver"""
        try:
            spark = self.spark_utils.get_spark_session()
            
            # Tables Silver requises
            required_tables = [
                'accidents_clean',
                'weather_aggregated', 
                'infrastructure_features'
            ]
            
            validation_results = {}
            
            for table_name in required_tables:
                try:
                    # Vérification existence table Hive
                    df = spark.table(f"{self.config_manager.get('hive.database')}.{table_name}")
                    count = df.count()
                    
                    if count == 0:
                        raise DataValidationError(f"Table {table_name} is empty")
                    
                    validation_results[table_name] = {
                        'exists': True,
                        'row_count': count,
                        'columns': len(df.columns)
                    }
                    
                    self.logger.info(f"Silver table validated", 
                                   table=table_name, 
                                   row_count=count,
                                   columns=len(df.columns))
                    
                except Exception as e:
                    validation_results[table_name] = {
                        'exists': False,
                        'error': str(e)
                    }
                    self.logger.error(f"Silver table validation failed", 
                                    table=table_name, exception=e)
                    raise
            
            self.processing_metrics['silver_validation'] = validation_results
            
        except Exception as e:
            self.logger.error("Silver data validation failed", exception=e)
            raise DataValidationError(f"Silver data validation failed: {str(e)}") from e
    
    def _perform_business_aggregations(self) -> Dict[str, DataFrame]:
        """Effectue les agrégations business multi-dimensionnelles"""
        try:
            self.logger.info("Starting business aggregations")
            start_time = time.time()
            
            # Agrégations par dimension
            aggregations = {}
            
            # 1. Agrégations temporelles
            aggregations['temporal'] = self.business_aggregator.aggregate_temporal_patterns()
            
            # 2. Agrégations géographiques
            aggregations['geographic'] = self.business_aggregator.aggregate_geographic_patterns()
            
            # 3. Agrégations météorologiques
            aggregations['weather'] = self.business_aggregator.aggregate_weather_patterns()
            
            # 4. Agrégations infrastructure
            aggregations['infrastructure'] = self.business_aggregator.aggregate_infrastructure_patterns()
            
            # 5. Agrégations croisées multi-dimensionnelles
            aggregations['cross_dimensional'] = self.business_aggregator.aggregate_cross_dimensional()
            
            duration = time.time() - start_time
            
            # Métriques d'agrégation
            agg_metrics = {}
            for key, df in aggregations.items():
                if df is not None:
                    agg_metrics[f"{key}_rows"] = df.count()
                    agg_metrics[f"{key}_columns"] = len(df.columns)
            
            self.processing_metrics['business_aggregations'] = {
                'duration_seconds': duration,
                'aggregations_count': len(aggregations),
                **agg_metrics
            }
            
            self.logger.log_performance("business_aggregations", duration, **agg_metrics)
            
            return aggregations
            
        except Exception as e:
            self.logger.error("Business aggregations failed", exception=e)
            raise SparkJobError(f"Business aggregations failed: {str(e)}") from e
    
    def _calculate_kpis(self, aggregated_data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Calcule tous les KPIs business"""
        try:
            self.logger.info("Starting KPI calculations")
            start_time = time.time()
            
            kpis = {}
            
            # 1. KPIs Sécurité
            kpis['security'] = self.kpi_calculator.calculate_security_kpis(aggregated_data)
            
            # 2. KPIs Temporels
            kpis['temporal'] = self.kpi_calculator.calculate_temporal_kpis(aggregated_data)
            
            # 3. KPIs Infrastructure
            kpis['infrastructure'] = self.kpi_calculator.calculate_infrastructure_kpis(aggregated_data)
            
            # 4. Hotspots (zones dangereuses)
            kpis['hotspots'] = self.kpi_calculator.calculate_hotspots(aggregated_data)
            
            # 5. Performance ML (si disponible)
            kpis['ml_performance'] = self.kpi_calculator.calculate_ml_performance_kpis()
            
            duration = time.time() - start_time
            
            # Métriques KPIs
            kpi_metrics = {}
            for key, df in kpis.items():
                if df is not None:
                    kpi_metrics[f"{key}_kpis_count"] = df.count()
            
            self.processing_metrics['kpi_calculations'] = {
                'duration_seconds': duration,
                'kpi_types_count': len(kpis),
                **kpi_metrics
            }
            
            self.logger.log_performance("kpi_calculations", duration, **kpi_metrics)
            
            return kpis
            
        except Exception as e:
            self.logger.error("KPI calculations failed", exception=e)
            raise SparkJobError(f"KPI calculations failed: {str(e)}") from e
    
    def _export_to_mysql(self, kpis_data: Dict[str, DataFrame]) -> Dict[str, Any]:
        """Exporte toutes les données vers MySQL"""
        try:
            self.logger.info("Starting MySQL export")
            start_time = time.time()
            
            export_results = {}
            
            # 1. Création des tables MySQL si nécessaire
            self.mysql_exporter.create_gold_tables()
            
            # 2. Export des agrégations principales
            export_results['accidents_summary'] = self.mysql_exporter.export_accidents_summary(
                kpis_data
            )
            
            # 3. Export des KPIs par catégorie
            for kpi_type, df in kpis_data.items():
                if df is not None and df.count() > 0:
                    export_results[kpi_type] = self.mysql_exporter.export_kpi_data(
                        kpi_type, df
                    )
            
            duration = time.time() - start_time
            
            # Métriques d'export
            total_exported = sum(result.get('rows_exported', 0) 
                               for result in export_results.values())
            
            self.processing_metrics['mysql_export'] = {
                'duration_seconds': duration,
                'tables_exported': len(export_results),
                'total_rows_exported': total_exported
            }
            
            self.logger.log_performance("mysql_export", duration, 
                                      tables_exported=len(export_results),
                                      total_rows_exported=total_exported)
            
            return export_results
            
        except Exception as e:
            self.logger.error("MySQL export failed", exception=e)
            raise FileProcessingError(f"MySQL export failed: {str(e)}") from e
    
    def _optimize_mysql_tables(self):
        """Optimise les tables MySQL pour les performances API"""
        try:
            self.logger.info("Starting MySQL table optimization")
            start_time = time.time()
            
            # 1. Création des index optimaux
            self.table_optimizer.create_optimal_indexes()
            
            # 2. Optimisation des requêtes
            self.table_optimizer.optimize_query_performance()
            
            # 3. Analyse des statistiques
            stats = self.table_optimizer.analyze_table_statistics()
            
            # 4. Recommandations d'optimisation
            recommendations = self.table_optimizer.generate_optimization_recommendations()
            
            duration = time.time() - start_time
            
            self.processing_metrics['mysql_optimization'] = {
                'duration_seconds': duration,
                'indexes_created': stats.get('indexes_created', 0),
                'tables_optimized': stats.get('tables_optimized', 0),
                'recommendations_count': len(recommendations)
            }
            
            self.logger.log_performance("mysql_optimization", duration, **stats)
            
        except Exception as e:
            self.logger.error("MySQL optimization failed", exception=e)
            # Non-bloquant, on continue même si l'optimisation échoue
            self.logger.warning("Continuing without optimization")
    
    def _generate_final_metrics(self, export_results: Dict[str, Any]) -> Dict[str, Any]:
        """Génère les métriques finales du pipeline"""
        total_duration = time.time() - self.start_time
        
        final_metrics = {
            'pipeline_duration_seconds': total_duration,
            'pipeline_status': 'SUCCESS',
            'timestamp': datetime.now().isoformat(),
            'processing_stages': self.processing_metrics,
            'export_summary': {
                'tables_created': len(export_results),
                'total_rows_exported': sum(
                    result.get('rows_exported', 0) 
                    for result in export_results.values()
                )
            }
        }
        
        # Ajout des métriques de qualité
        final_metrics['data_quality'] = self.analytics_engine.calculate_data_quality_metrics()
        
        return final_metrics
    
    def _cleanup_resources(self):
        """Nettoie les ressources utilisées"""
        try:
            if hasattr(self, 'spark_utils') and self.spark_utils:
                # Cache cleanup
                spark = self.spark_utils.get_spark_session()
                spark.catalog.clearCache()
            
            # Database connections cleanup
            if hasattr(self, 'db_connector') and self.db_connector:
                self.db_connector.close()
            
            self.logger.info("Resources cleaned up successfully")
            
        except Exception as e:
            self.logger.warning("Resource cleanup failed", exception=e)
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Retourne le statut actuel du pipeline"""
        return {
            'status': 'RUNNING' if self.start_time else 'IDLE',
            'start_time': self.start_time,
            'current_metrics': self.processing_metrics,
            'components_initialized': all([
                self.business_aggregator is not None,
                self.kpi_calculator is not None,
                self.mysql_exporter is not None,
                self.table_optimizer is not None,
                self.analytics_engine is not None
            ])
        }
    
    def run_incremental_update(self, since_timestamp: str) -> Dict[str, Any]:
        """
        Exécute une mise à jour incrémentale depuis un timestamp donné
        
        Args:
            since_timestamp: Timestamp ISO format pour la mise à jour incrémentale
            
        Returns:
            Dict contenant les métriques de la mise à jour
        """
        try:
            self.logger.info("Starting incremental DATAMART update", 
                           since_timestamp=since_timestamp)
            
            # Initialisation des composants si nécessaire
            if not self.business_aggregator:
                self._initialize_components()
            
            # Mise à jour incrémentale des agrégations
            updated_data = self.business_aggregator.incremental_aggregation(since_timestamp)
            
            # Recalcul des KPIs affectés
            updated_kpis = self.kpi_calculator.incremental_kpi_update(updated_data)
            
            # Export incrémental vers MySQL
            export_results = self.mysql_exporter.incremental_export(updated_kpis)
            
            self.logger.info("Incremental DATAMART update completed", 
                           **export_results)
            
            return export_results
            
        except Exception as e:
            self.logger.error("Incremental DATAMART update failed", exception=e)
            raise LakehouseException(f"Incremental update failed: {str(e)}") from e


if __name__ == "__main__":
    """Point d'entrée pour exécution standalone"""
    import argparse
    
    parser = argparse.ArgumentParser(description="DATAMART Application")
    parser.add_argument("--force-refresh", action="store_true", 
                       help="Force refresh of all aggregations")
    parser.add_argument("--incremental", type=str, 
                       help="Run incremental update since timestamp (ISO format)")
    
    args = parser.parse_args()
    
    app = DatamartApp()
    
    try:
        if args.incremental:
            result = app.run_incremental_update(args.incremental)
        else:
            result = app.run(force_refresh=args.force_refresh)
        
        print(f"DATAMART pipeline completed successfully")
        print(f"Total duration: {result['pipeline_duration_seconds']:.2f} seconds")
        print(f"Tables exported: {result['export_summary']['tables_created']}")
        print(f"Rows exported: {result['export_summary']['total_rows_exported']}")
        
    except Exception as e:
        print(f"DATAMART pipeline failed: {str(e)}")
        exit(1)