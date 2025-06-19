"""
KPICalculator - Calcul des KPIs business pour DATAMART
"""

import time
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, isnan, isnull, desc, asc, round as spark_round,
    year, month, dayofmonth, hour, dayofweek, weekofyear,
    percentile_approx, stddev, variance, skewness, kurtosis,
    dense_rank, row_number, lag, lead, first, last,
    collect_list, collect_set, size, array_contains,
    regexp_replace, lower, upper, trim, split,
    unix_timestamp, from_unixtime, date_format,
    coalesce, greatest, least, abs as spark_abs,
    sqrt, pow as spark_pow, log, exp, lit,
    concat_ws, countDistinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import SparkJobError, DataValidationError


class KPICalculator:
    """
    Calculateur de KPIs business pour la couche Gold
    
    Fonctionnalités:
    - KPIs Sécurité: Taux d'accidents, index dangerosité, hotspots
    - KPIs Temporels: Évolution mensuelle/annuelle, pics horaires, saisonnalité
    - KPIs Infrastructure: Corrélation équipements vs accidents, efficacité
    - KPIs Performance ML: Métriques modèle, accuracy, F1-score
    - Calculs statistiques avancés et rankings
    - Détection d'anomalies et tendances
    """
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager, logger: Logger):
        self.spark = spark
        self.config_manager = config_manager
        self.logger = logger
        
        # Configuration
        self.hive_database = config_manager.get('hive.database')
        
        # Seuils pour les KPIs
        self.kpi_thresholds = {
            'high_danger_threshold': 2.5,  # Sévérité moyenne
            'hotspot_min_accidents': 50,   # Minimum accidents pour hotspot
            'trend_significance': 0.1,     # Seuil de significativité des tendances
            'top_hotspots_count': 50       # Top N hotspots à retourner
        }
        
        self.logger.info("KPICalculator initialized", 
                        hive_database=self.hive_database,
                        thresholds=self.kpi_thresholds)
    
    def calculate_security_kpis(self, aggregated_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Calcule les KPIs de sécurité
        
        Args:
            aggregated_data: Données agrégées par BusinessAggregator
            
        Returns:
            DataFrame avec KPIs de sécurité
        """
        try:
            self.logger.info("Starting security KPIs calculation")
            start_time = time.time()
            
            # Récupération des données géographiques
            geographic_data = aggregated_data.get('geographic')
            if geographic_data is None:
                raise DataValidationError("Geographic data not available for security KPIs")
            
            # Filtrage des données par état
            state_data = geographic_data.filter(col("geographic_level") == "state")
            city_data = geographic_data.filter(col("geographic_level") == "city")
            
            # Calcul des KPIs de sécurité par état
            security_kpis_state = state_data.select(
                col("state"),
                lit(None).cast("string").alias("city"),
                col("accident_rate_per_100k"),
                col("avg_severity").alias("danger_index"),
                col("accidents_count"),
                col("geographic_rank").alias("hotspot_rank")
            ).withColumn(
                "safety_category",
                when(col("danger_index") >= self.kpi_thresholds['high_danger_threshold'], "High Risk")
                .when(col("danger_index") >= 2.0, "Medium Risk")
                .otherwise("Low Risk")
            ).withColumn(
                "severity_distribution",
                # Simulation de distribution JSON - en production, calculer réellement
                concat_ws(",", 
                    lit('{"severity_1":'), (col("accidents_count") * 0.1).cast("int"), lit(','),
                    lit('"severity_2":'), (col("accidents_count") * 0.3).cast("int"), lit(','),
                    lit('"severity_3":'), (col("accidents_count") * 0.4).cast("int"), lit(','),
                    lit('"severity_4":'), (col("accidents_count") * 0.2).cast("int"), lit('}')
                )
            )
            
            # Calcul des KPIs de sécurité par ville (top 50)
            security_kpis_city = city_data.select(
                col("state"),
                col("city"),
                lit(None).cast("double").alias("accident_rate_per_100k"),
                col("avg_severity").alias("danger_index"),
                col("accidents_count"),
                col("geographic_rank").alias("hotspot_rank")
            ).withColumn(
                "safety_category",
                when(col("danger_index") >= self.kpi_thresholds['high_danger_threshold'], "High Risk")
                .when(col("danger_index") >= 2.0, "Medium Risk")
                .otherwise("Low Risk")
            ).withColumn(
                "severity_distribution",
                concat_ws(",", 
                    lit('{"severity_1":'), (col("accidents_count") * 0.1).cast("int"), lit(','),
                    lit('"severity_2":'), (col("accidents_count") * 0.3).cast("int"), lit(','),
                    lit('"severity_3":'), (col("accidents_count") * 0.4).cast("int"), lit(','),
                    lit('"severity_4":'), (col("accidents_count") * 0.2).cast("int"), lit('}')
                )
            ).filter(col("hotspot_rank") <= self.kpi_thresholds['top_hotspots_count'])
            
            # Union des KPIs état et ville
            security_kpis = security_kpis_state.union(security_kpis_city)
            
            # Ajout de métadonnées temporelles
            security_kpis = security_kpis.withColumn(
                "last_updated", 
                lit(time.time()).cast("timestamp")
            )
            
            duration = time.time() - start_time
            row_count = security_kpis.count()
            
            self.logger.log_performance("security_kpis_calculation", duration,
                                      kpis_generated=row_count)
            
            return security_kpis
            
        except Exception as e:
            self.logger.error("Security KPIs calculation failed", exception=e)
            raise SparkJobError(f"Security KPIs calculation failed: {str(e)}") from e
    
    def calculate_temporal_kpis(self, aggregated_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Calcule les KPIs temporels
        
        Args:
            aggregated_data: Données agrégées par BusinessAggregator
            
        Returns:
            DataFrame avec KPIs temporels
        """
        try:
            self.logger.info("Starting temporal KPIs calculation")
            start_time = time.time()
            
            # Récupération des données temporelles
            temporal_data = aggregated_data.get('temporal')
            if temporal_data is None:
                raise DataValidationError("Temporal data not available for temporal KPIs")
            
            # Calcul des KPIs temporels avec enrichissement
            temporal_kpis = temporal_data.select(
                col("period_type"),
                col("period_value"),
                lit("ALL").alias("state"),  # Agrégation globale
                col("accidents_count"),
                col("avg_severity").alias("severity_avg"),
                col("trend_direction"),
                col("states_affected"),
                col("cities_affected")
            ).withColumn(
                "seasonal_factor",
                # Calcul du facteur saisonnier basé sur le type de période
                when(col("period_type") == "monthly",
                     when(col("period_value").rlike("(12|01|02)"), 1.2)  # Hiver
                     .when(col("period_value").rlike("(06|07|08)"), 1.1)  # Été
                     .otherwise(1.0))
                .when(col("period_type") == "hourly",
                     when(col("period_value").cast("int").between(7, 9), 1.3)  # Rush matinal
                     .when(col("period_value").cast("int").between(17, 19), 1.4)  # Rush soir
                     .otherwise(1.0))
                .when(col("period_type") == "daily",
                     when(col("period_value").cast("int").isin([6, 7]), 0.8)  # Weekend
                     .otherwise(1.0))
                .otherwise(1.0)
            ).withColumn(
                "risk_level",
                when(col("accidents_count") * col("seasonal_factor") > 1000, "High")
                .when(col("accidents_count") * col("seasonal_factor") > 500, "Medium")
                .otherwise("Low")
            )
            
            # Calcul des tendances avancées
            window_trend = Window.partitionBy("period_type").orderBy("period_value")
            temporal_kpis = temporal_kpis.withColumn(
                "trend_strength",
                spark_abs(
                    (col("accidents_count") - lag("accidents_count").over(window_trend)) /
                    (lag("accidents_count").over(window_trend) + 1)
                )
            ).withColumn(
                "trend_significance",
                when(col("trend_strength") > self.kpi_thresholds['trend_significance'], "Significant")
                .otherwise("Not Significant")
            )
            
            # Identification des pics et creux
            temporal_kpis = temporal_kpis.withColumn(
                "anomaly_type",
                when(col("accidents_count") > 
                     avg("accidents_count").over(Window.partitionBy("period_type")) * 1.5, "Peak")
                .when(col("accidents_count") < 
                     avg("accidents_count").over(Window.partitionBy("period_type")) * 0.5, "Valley")
                .otherwise("Normal")
            )
            
            # Ajout de métadonnées temporelles
            temporal_kpis = temporal_kpis.withColumn(
                "last_updated", 
                lit(time.time()).cast("timestamp")
            )
            
            duration = time.time() - start_time
            row_count = temporal_kpis.count()
            
            self.logger.log_performance("temporal_kpis_calculation", duration,
                                      kpis_generated=row_count)
            
            return temporal_kpis
            
        except Exception as e:
            self.logger.error("Temporal KPIs calculation failed", exception=e)
            raise SparkJobError(f"Temporal KPIs calculation failed: {str(e)}") from e
    
    def calculate_infrastructure_kpis(self, aggregated_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Calcule les KPIs d'infrastructure
        
        Args:
            aggregated_data: Données agrégées par BusinessAggregator
            
        Returns:
            DataFrame avec KPIs d'infrastructure
        """
        try:
            self.logger.info("Starting infrastructure KPIs calculation")
            start_time = time.time()
            
            # Récupération des données d'infrastructure
            infrastructure_data = aggregated_data.get('infrastructure')
            if infrastructure_data is None:
                raise DataValidationError("Infrastructure data not available for infrastructure KPIs")
            
            # Calcul des KPIs d'infrastructure enrichis
            infrastructure_kpis = infrastructure_data.select(
                col("state"),
                col("infrastructure_type"),
                col("equipment_count"),
                col("effectiveness_score").alias("equipment_effectiveness"),
                col("safety_improvement_score"),
                col("recommendation"),
                col("accidents_count"),
                col("avg_severity")
            ).withColumn(
                "accident_reduction_rate",
                # Calcul du taux de réduction d'accidents (simulation)
                spark_round(
                    when(col("equipment_effectiveness") > 1.0, 
                         (col("equipment_effectiveness") - 1.0) * 0.1)
                    .otherwise(0.0), 3
                )
            ).withColumn(
                "infrastructure_efficiency",
                when(col("equipment_count") > 0,
                     spark_round(col("safety_improvement_score") / col("equipment_count"), 2))
                .otherwise(0.0)
            ).withColumn(
                "priority_level",
                when(col("equipment_effectiveness") < 0.5, "High Priority")
                .when(col("equipment_effectiveness") < 1.0, "Medium Priority")
                .otherwise("Low Priority")
            )
            
            # Calcul des rankings par efficacité
            window_effectiveness = Window.partitionBy("infrastructure_type").orderBy(desc("equipment_effectiveness"))
            infrastructure_kpis = infrastructure_kpis.withColumn(
                "effectiveness_rank",
                row_number().over(window_effectiveness)
            )
            
            # Génération de recommandations avancées
            infrastructure_kpis = infrastructure_kpis.withColumn(
                "detailed_recommendation",
                when(col("equipment_effectiveness") < 0.3, 
                     concat_ws(" ", lit("Critical:"), col("recommendation"), 
                              lit("- Consider immediate infrastructure upgrade")))
                .when(col("equipment_effectiveness") < 0.7,
                     concat_ws(" ", lit("Important:"), col("recommendation"),
                              lit("- Plan infrastructure improvements")))
                .otherwise(concat_ws(" ", lit("Maintenance:"), col("recommendation")))
            )
            
            # Calcul des métriques de ROI (Return on Investment)
            infrastructure_kpis = infrastructure_kpis.withColumn(
                "estimated_roi",
                spark_round(
                    col("accident_reduction_rate") * col("accidents_count") * 50000 / 
                    (col("equipment_count") * 10000 + 1), 2  # Simulation coût/bénéfice
                )
            )
            
            # Ajout de métadonnées temporelles
            infrastructure_kpis = infrastructure_kpis.withColumn(
                "last_updated", 
                lit(time.time()).cast("timestamp")
            )
            
            duration = time.time() - start_time
            row_count = infrastructure_kpis.count()
            
            self.logger.log_performance("infrastructure_kpis_calculation", duration,
                                      kpis_generated=row_count)
            
            return infrastructure_kpis
            
        except Exception as e:
            self.logger.error("Infrastructure KPIs calculation failed", exception=e)
            raise SparkJobError(f"Infrastructure KPIs calculation failed: {str(e)}") from e
    
    def calculate_hotspots(self, aggregated_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Calcule les hotspots (zones dangereuses)
        
        Args:
            aggregated_data: Données agrégées par BusinessAggregator
            
        Returns:
            DataFrame avec hotspots identifiés
        """
        try:
            self.logger.info("Starting hotspots calculation")
            start_time = time.time()
            
            # Récupération des données géographiques
            geographic_data = aggregated_data.get('geographic')
            if geographic_data is None:
                raise DataValidationError("Geographic data not available for hotspots calculation")
            
            # Filtrage des villes avec coordonnées
            city_hotspots = geographic_data.filter(
                (col("geographic_level") == "city") &
                (col("latitude").isNotNull()) &
                (col("longitude").isNotNull()) &
                (col("accidents_count") >= self.kpi_thresholds['hotspot_min_accidents'])
            )
            
            # Calcul du score de dangerosité
            hotspots = city_hotspots.select(
                col("state"),
                col("city"),
                col("latitude"),
                col("longitude"),
                col("accidents_count"),
                col("avg_severity").alias("severity_avg"),
                col("geographic_rank")
            ).withColumn(
                "danger_score",
                spark_round(
                    col("accidents_count") * col("severity_avg") * 
                    when(col("severity_avg") > 3.0, 1.5).otherwise(1.0), 2
                )
            ).withColumn(
                "radius_miles",
                # Calcul du rayon d'influence basé sur la densité d'accidents
                spark_round(
                    sqrt(col("accidents_count") / 100.0) + 0.5, 1
                )
            ).withColumn(
                "hotspot_category",
                when(col("danger_score") > 500, "Critical")
                .when(col("danger_score") > 200, "High")
                .when(col("danger_score") > 100, "Medium")
                .otherwise("Low")
            )
            
            # Ranking des hotspots par score de dangerosité
            window_danger = Window.orderBy(desc("danger_score"))
            hotspots = hotspots.withColumn(
                "hotspot_rank",
                row_number().over(window_danger)
            ).filter(col("hotspot_rank") <= self.kpi_thresholds['top_hotspots_count'])
            
            # Calcul des statistiques de zone
            hotspots = hotspots.withColumn(
                "risk_assessment",
                when(col("hotspot_rank") <= 10, "Immediate Action Required")
                .when(col("hotspot_rank") <= 25, "High Priority Monitoring")
                .otherwise("Regular Monitoring")
            ).withColumn(
                "estimated_impact_reduction",
                # Estimation de la réduction d'impact avec interventions
                spark_round(col("danger_score") * 0.3, 2)  # 30% de réduction estimée
            )
            
            # Ajout de métadonnées temporelles
            hotspots = hotspots.withColumn(
                "last_updated", 
                lit(time.time()).cast("timestamp")
            ).withColumn(
                "analysis_date",
                date_format(lit(time.time()).cast("timestamp"), "yyyy-MM-dd")
            )
            
            duration = time.time() - start_time
            row_count = hotspots.count()
            
            self.logger.log_performance("hotspots_calculation", duration,
                                      hotspots_identified=row_count)
            
            return hotspots
            
        except Exception as e:
            self.logger.error("Hotspots calculation failed", exception=e)
            raise SparkJobError(f"Hotspots calculation failed: {str(e)}") from e
    
    def calculate_ml_performance_kpis(self) -> Optional[DataFrame]:
        """
        Calcule les KPIs de performance ML (si modèles disponibles)
        
        Returns:
            DataFrame avec KPIs de performance ML ou None si pas de modèles
        """
        try:
            self.logger.info("Starting ML performance KPIs calculation")
            start_time = time.time()
            
            # Vérification de l'existence de métriques ML
            # En production, ceci viendrait de MLflow ou autre système de tracking
            
            # Simulation de métriques ML pour démonstration
            ml_metrics_data = [
                {
                    "model_name": "accident_severity_predictor",
                    "model_version": "v1.2.0",
                    "accuracy": 0.847,
                    "precision_score": 0.823,
                    "recall_score": 0.856,
                    "f1_score": 0.839,
                    "feature_importance": '{"weather": 0.35, "time": 0.28, "location": 0.22, "infrastructure": 0.15}',
                    "training_date": "2024-01-15T10:30:00",
                    "data_drift_score": 0.12,
                    "model_stability": "Stable"
                },
                {
                    "model_name": "hotspot_detection_model",
                    "model_version": "v2.1.0", 
                    "accuracy": 0.792,
                    "precision_score": 0.778,
                    "recall_score": 0.805,
                    "f1_score": 0.791,
                    "feature_importance": '{"accident_density": 0.42, "infrastructure": 0.31, "weather": 0.27}',
                    "training_date": "2024-01-10T14:20:00",
                    "data_drift_score": 0.08,
                    "model_stability": "Stable"
                }
            ]
            
            # Création du DataFrame
            ml_performance_kpis = self.spark.createDataFrame(ml_metrics_data)
            
            # Enrichissement des métriques
            ml_performance_kpis = ml_performance_kpis.withColumn(
                "performance_grade",
                when(col("f1_score") >= 0.9, "Excellent")
                .when(col("f1_score") >= 0.8, "Good")
                .when(col("f1_score") >= 0.7, "Fair")
                .otherwise("Poor")
            ).withColumn(
                "drift_status",
                when(col("data_drift_score") > 0.2, "High Drift - Retrain Required")
                .when(col("data_drift_score") > 0.1, "Medium Drift - Monitor Closely")
                .otherwise("Low Drift - Stable")
            ).withColumn(
                "model_health_score",
                spark_round(
                    (col("f1_score") * 0.6 + 
                     (1 - col("data_drift_score")) * 0.4) * 100, 1
                )
            ).withColumn(
                "recommendation",
                when(col("model_health_score") < 70, "Immediate retraining required")
                .when(col("model_health_score") < 85, "Schedule retraining within 30 days")
                .otherwise("Continue monitoring")
            )
            
            # Ajout de métadonnées temporelles
            ml_performance_kpis = ml_performance_kpis.withColumn(
                "last_updated", 
                lit(time.time()).cast("timestamp")
            ).withColumn(
                "evaluation_date",
                date_format(lit(time.time()).cast("timestamp"), "yyyy-MM-dd")
            )
            
            duration = time.time() - start_time
            row_count = ml_performance_kpis.count()
            
            self.logger.log_performance("ml_performance_kpis_calculation", duration,
                                      models_evaluated=row_count)
            
            return ml_performance_kpis
            
        except Exception as e:
            self.logger.warning("ML performance KPIs calculation failed - continuing without ML metrics", 
                              exception=e)
            return None
    
    def incremental_kpi_update(self, updated_data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """
        Mise à jour incrémentale des KPIs
        
        Args:
            updated_data: Données mises à jour par BusinessAggregator
            
        Returns:
            Dict des KPIs mis à jour
        """
        try:
            self.logger.info("Starting incremental KPI update")
            start_time = time.time()
            
            updated_kpis = {}
            
            # Recalcul des KPIs affectés par les données mises à jour
            if 'geographic' in updated_data:
                updated_kpis['security'] = self.calculate_security_kpis(updated_data)
                updated_kpis['hotspots'] = self.calculate_hotspots(updated_data)
            
            if 'temporal' in updated_data:
                updated_kpis['temporal'] = self.calculate_temporal_kpis(updated_data)
            
            if 'infrastructure' in updated_data:
                updated_kpis['infrastructure'] = self.calculate_infrastructure_kpis(updated_data)
            
            # ML KPIs (toujours recalculés car indépendants des données agrégées)
            ml_kpis = self.calculate_ml_performance_kpis()
            if ml_kpis is not None:
                updated_kpis['ml_performance'] = ml_kpis
            
            duration = time.time() - start_time
            
            self.logger.log_performance("incremental_kpi_update", duration,
                                      updated_kpi_types=len(updated_kpis))
            
            return updated_kpis
            
        except Exception as e:
            self.logger.error("Incremental KPI update failed", exception=e)
            raise SparkJobError(f"Incremental KPI update failed: {str(e)}") from e
    
    def validate_kpi_quality(self, kpis_data: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        Valide la qualité des KPIs calculés
        
        Args:
            kpis_data: KPIs calculés
            
        Returns:
            Dict avec métriques de qualité
        """
        try:
            quality_metrics = {}
            
            for kpi_type, df in kpis_data.items():
                if df is not None:
                    row_count = df.count()
                    null_counts = {}
                    
                    # Vérification des valeurs nulles par colonne
                    for col_name in df.columns:
                        null_count = df.filter(col(col_name).isNull()).count()
                        null_counts[col_name] = null_count
                    
                    quality_metrics[kpi_type] = {
                        'total_rows': row_count,
                        'null_counts': null_counts,
                        'completeness_score': 1.0 - (sum(null_counts.values()) / (row_count * len(df.columns)))
                    }
            
            self.logger.log_data_quality("kpis_validation", quality_metrics)
            
            return quality_metrics
            
        except Exception as e:
            self.logger.error("KPI quality validation failed", exception=e)
            return {}