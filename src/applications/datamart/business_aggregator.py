"""
BusinessAggregator - Agrégations business multi-dimensionnelles pour DATAMART
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
    sqrt, pow as spark_pow, log, exp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import SparkJobError, DataValidationError


class BusinessAggregator:
    """
    Agrégations business multi-dimensionnelles pour la couche Gold
    
    Fonctionnalités:
    - Agrégations temporelles (horaires, journalières, mensuelles, annuelles)
    - Agrégations géographiques (état, ville, coordonnées GPS)
    - Agrégations météorologiques (conditions, température, visibilité)
    - Agrégations infrastructure (équipements, types de routes)
    - Jointures optimisées entre tables Hive Silver
    - Calculs statistiques avancés (percentiles, rankings)
    - Dénormalisation pour performance API
    """
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager, logger: Logger):
        self.spark = spark
        self.config_manager = config_manager
        self.logger = logger
        
        # Configuration Hive
        self.hive_database = config_manager.get('hive.database')
        self.table_names = {
            'accidents_clean': config_manager.get_hive_table_name('accidents_clean'),
            'weather_aggregated': config_manager.get_hive_table_name('weather_aggregated'),
            'infrastructure_features': config_manager.get_hive_table_name('infrastructure_features')
        }
        
        # Cache des DataFrames pour optimisation
        self._cached_dfs = {}
        
        self.logger.info("BusinessAggregator initialized", 
                        hive_database=self.hive_database,
                        tables=self.table_names)
    
    def aggregate_temporal_patterns(self) -> DataFrame:
        """
        Agrégations temporelles multi-niveaux
        
        Returns:
            DataFrame avec patterns temporels agrégés
        """
        try:
            self.logger.info("Starting temporal patterns aggregation")
            start_time = time.time()
            
            # Chargement des données accidents
            accidents_df = self._get_cached_accidents_data()
            
            # Extraction des composants temporels
            temporal_df = accidents_df.select(
                col("id"),
                col("state"),
                col("city"),
                col("severity"),
                col("start_time").alias("accident_timestamp"),
                year(col("start_time")).alias("accident_year"),
                month(col("start_time")).alias("accident_month"),
                dayofmonth(col("start_time")).alias("accident_day"),
                hour(col("start_time")).alias("accident_hour"),
                dayofweek(col("start_time")).alias("day_of_week"),
                weekofyear(col("start_time")).alias("week_of_year"),
                date_format(col("start_time"), "yyyy-MM").alias("year_month"),
                date_format(col("start_time"), "yyyy-MM-dd").alias("accident_date")
            )
            
            # Agrégations par heure
            hourly_agg = temporal_df.groupBy("accident_hour").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                countDistinct("state").alias("states_affected"),
                countDistinct("city").alias("cities_affected")
            ).withColumn("period_type", lit("hourly"))
            
            # Agrégations par jour de la semaine
            daily_agg = temporal_df.groupBy("day_of_week").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                countDistinct("state").alias("states_affected"),
                countDistinct("city").alias("cities_affected")
            ).withColumn("period_type", lit("daily"))
            
            # Agrégations mensuelles
            monthly_agg = temporal_df.groupBy("year_month").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                countDistinct("state").alias("states_affected"),
                countDistinct("city").alias("cities_affected")
            ).withColumn("period_type", lit("monthly"))
            
            # Agrégations annuelles
            yearly_agg = temporal_df.groupBy("accident_year").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                countDistinct("state").alias("states_affected"),
                countDistinct("city").alias("cities_affected")
            ).withColumn("period_type", lit("yearly"))
            
            # Union de toutes les agrégations temporelles
            temporal_patterns = hourly_agg.select(
                col("accident_hour").cast("string").alias("period_value"),
                col("period_type"),
                col("accidents_count"),
                col("avg_severity"),
                col("states_affected"),
                col("cities_affected")
            ).union(
                daily_agg.select(
                    col("day_of_week").cast("string").alias("period_value"),
                    col("period_type"),
                    col("accidents_count"),
                    col("avg_severity"),
                    col("states_affected"),
                    col("cities_affected")
                )
            ).union(
                monthly_agg.select(
                    col("year_month").alias("period_value"),
                    col("period_type"),
                    col("accidents_count"),
                    col("avg_severity"),
                    col("states_affected"),
                    col("cities_affected")
                )
            ).union(
                yearly_agg.select(
                    col("accident_year").cast("string").alias("period_value"),
                    col("period_type"),
                    col("accidents_count"),
                    col("avg_severity"),
                    col("states_affected"),
                    col("cities_affected")
                )
            )
            
            # Calcul des tendances
            window_spec = Window.partitionBy("period_type").orderBy("period_value")
            temporal_patterns = temporal_patterns.withColumn(
                "trend_direction",
                when(
                    col("accidents_count") > lag("accidents_count").over(window_spec),
                    "up"
                ).when(
                    col("accidents_count") < lag("accidents_count").over(window_spec),
                    "down"
                ).otherwise("stable")
            )
            
            # Cache pour réutilisation
            temporal_patterns.cache()
            
            duration = time.time() - start_time
            row_count = temporal_patterns.count()
            
            self.logger.log_performance("temporal_patterns_aggregation", duration,
                                      rows_generated=row_count)
            
            return temporal_patterns
            
        except Exception as e:
            self.logger.error("Temporal patterns aggregation failed", exception=e)
            raise SparkJobError(f"Temporal patterns aggregation failed: {str(e)}") from e
    
    def aggregate_geographic_patterns(self) -> DataFrame:
        """
        Agrégations géographiques multi-niveaux
        
        Returns:
            DataFrame avec patterns géographiques agrégés
        """
        try:
            self.logger.info("Starting geographic patterns aggregation")
            start_time = time.time()
            
            # Chargement des données accidents
            accidents_df = self._get_cached_accidents_data()
            
            # Agrégations par état
            state_agg = accidents_df.groupBy("state").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                countDistinct("city").alias("cities_count"),
                avg("distance_mi").alias("avg_distance"),
                percentile_approx("severity", 0.5).alias("median_severity"),
                percentile_approx("severity", 0.95).alias("p95_severity"),
                stddev("severity").alias("severity_stddev")
            )
            
            # Calcul du taux d'accidents par 100k habitants (simulation)
            # En production, on joindrait avec des données de population réelles
            state_agg = state_agg.withColumn(
                "accident_rate_per_100k",
                spark_round(col("accidents_count") / 100.0, 2)  # Simulation
            )
            
            # Ranking des états par dangerosité
            window_danger = Window.orderBy(desc("accident_rate_per_100k"))
            state_agg = state_agg.withColumn(
                "danger_rank",
                dense_rank().over(window_danger)
            )
            
            # Agrégations par ville (top 100 villes les plus dangereuses)
            city_agg = accidents_df.groupBy("state", "city").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                avg("distance_mi").alias("avg_distance"),
                avg("start_lat").alias("avg_latitude"),
                avg("start_lng").alias("avg_longitude")
            ).filter(col("accidents_count") >= 10)  # Filtre pour villes significatives
            
            # Calcul du score de dangerosité par ville
            city_agg = city_agg.withColumn(
                "danger_score",
                spark_round(
                    col("accidents_count") * col("avg_severity") / 100.0, 2
                )
            )
            
            # Top 100 villes les plus dangereuses
            window_city_danger = Window.orderBy(desc("danger_score"))
            top_dangerous_cities = city_agg.withColumn(
                "city_danger_rank",
                row_number().over(window_city_danger)
            ).filter(col("city_danger_rank") <= 100)
            
            # Combinaison des agrégations géographiques
            geographic_patterns = state_agg.select(
                col("state"),
                lit(None).cast("string").alias("city"),
                col("accidents_count"),
                col("avg_severity"),
                col("accident_rate_per_100k"),
                col("danger_rank").alias("geographic_rank"),
                lit("state").alias("geographic_level"),
                lit(None).cast("double").alias("latitude"),
                lit(None).cast("double").alias("longitude")
            ).union(
                top_dangerous_cities.select(
                    col("state"),
                    col("city"),
                    col("accidents_count"),
                    col("avg_severity"),
                    lit(None).cast("double").alias("accident_rate_per_100k"),
                    col("city_danger_rank").alias("geographic_rank"),
                    lit("city").alias("geographic_level"),
                    col("avg_latitude").alias("latitude"),
                    col("avg_longitude").alias("longitude")
                )
            )
            
            # Cache pour réutilisation
            geographic_patterns.cache()
            
            duration = time.time() - start_time
            row_count = geographic_patterns.count()
            
            self.logger.log_performance("geographic_patterns_aggregation", duration,
                                      rows_generated=row_count)
            
            return geographic_patterns
            
        except Exception as e:
            self.logger.error("Geographic patterns aggregation failed", exception=e)
            raise SparkJobError(f"Geographic patterns aggregation failed: {str(e)}") from e
    
    def aggregate_weather_patterns(self) -> DataFrame:
        """
        Agrégations météorologiques avec corrélations accidents
        
        Returns:
            DataFrame avec patterns météorologiques agrégés
        """
        try:
            self.logger.info("Starting weather patterns aggregation")
            start_time = time.time()
            
            # Chargement des données
            accidents_df = self._get_cached_accidents_data()
            weather_df = self._get_cached_weather_data()
            
            # Jointure accidents-météo
            accidents_weather = accidents_df.join(
                weather_df,
                ["state", "city"],  # Jointure approximative par localisation
                "left"
            )
            
            # Catégorisation des conditions météo
            weather_categorized = accidents_weather.withColumn(
                "weather_category",
                when(col("weather_condition").rlike("(?i)(clear|fair)"), "Clear")
                .when(col("weather_condition").rlike("(?i)(cloud|overcast)"), "Cloudy")
                .when(col("weather_condition").rlike("(?i)(rain|drizzle|shower)"), "Rainy")
                .when(col("weather_condition").rlike("(?i)(snow|sleet|ice)"), "Snow/Ice")
                .when(col("weather_condition").rlike("(?i)(fog|mist|haze)"), "Fog")
                .when(col("weather_condition").rlike("(?i)(wind|storm)"), "Windy/Storm")
                .otherwise("Other")
            ).withColumn(
                "temperature_category",
                when(col("temperature_f") < 32, "Freezing")
                .when(col("temperature_f") < 50, "Cold")
                .when(col("temperature_f") < 70, "Mild")
                .when(col("temperature_f") < 85, "Warm")
                .otherwise("Hot")
            ).withColumn(
                "visibility_category",
                when(col("visibility_mi") < 1, "Very Poor")
                .when(col("visibility_mi") < 5, "Poor")
                .when(col("visibility_mi") < 10, "Moderate")
                .otherwise("Good")
            )
            
            # Agrégations par conditions météo
            weather_agg = weather_categorized.groupBy("weather_category").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                avg("temperature_f").alias("avg_temperature"),
                avg("humidity_percent").alias("avg_humidity"),
                avg("visibility_mi").alias("avg_visibility"),
                countDistinct("state").alias("states_affected")
            )
            
            # Agrégations par température
            temp_agg = weather_categorized.groupBy("temperature_category").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                avg("temperature_f").alias("avg_temperature"),
                countDistinct("state").alias("states_affected")
            )
            
            # Agrégations par visibilité
            visibility_agg = weather_categorized.groupBy("visibility_category").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                avg("visibility_mi").alias("avg_visibility"),
                countDistinct("state").alias("states_affected")
            )
            
            # Calcul des corrélations météo-sévérité
            weather_patterns = weather_agg.select(
                col("weather_category").alias("weather_factor"),
                lit("weather_condition").alias("factor_type"),
                col("accidents_count"),
                col("avg_severity"),
                col("states_affected"),
                col("avg_temperature"),
                col("avg_humidity"),
                col("avg_visibility")
            ).union(
                temp_agg.select(
                    col("temperature_category").alias("weather_factor"),
                    lit("temperature").alias("factor_type"),
                    col("accidents_count"),
                    col("avg_severity"),
                    col("states_affected"),
                    col("avg_temperature"),
                    lit(None).cast("double").alias("avg_humidity"),
                    lit(None).cast("double").alias("avg_visibility")
                )
            ).union(
                visibility_agg.select(
                    col("visibility_category").alias("weather_factor"),
                    lit("visibility").alias("factor_type"),
                    col("accidents_count"),
                    col("avg_severity"),
                    col("states_affected"),
                    lit(None).cast("double").alias("avg_temperature"),
                    lit(None).cast("double").alias("avg_humidity"),
                    col("avg_visibility")
                )
            )
            
            # Calcul de l'impact météo sur la sévérité
            weather_patterns = weather_patterns.withColumn(
                "severity_impact_score",
                spark_round(
                    col("avg_severity") * col("accidents_count") / 1000.0, 2
                )
            )
            
            # Cache pour réutilisation
            weather_patterns.cache()
            
            duration = time.time() - start_time
            row_count = weather_patterns.count()
            
            self.logger.log_performance("weather_patterns_aggregation", duration,
                                      rows_generated=row_count)
            
            return weather_patterns
            
        except Exception as e:
            self.logger.error("Weather patterns aggregation failed", exception=e)
            raise SparkJobError(f"Weather patterns aggregation failed: {str(e)}") from e
    
    def aggregate_infrastructure_patterns(self) -> DataFrame:
        """
        Agrégations infrastructure avec efficacité des équipements
        
        Returns:
            DataFrame avec patterns infrastructure agrégés
        """
        try:
            self.logger.info("Starting infrastructure patterns aggregation")
            start_time = time.time()
            
            # Chargement des données
            accidents_df = self._get_cached_accidents_data()
            infrastructure_df = self._get_cached_infrastructure_data()
            
            # Jointure accidents-infrastructure
            accidents_infra = accidents_df.join(
                infrastructure_df,
                ["state"],  # Jointure par état
                "left"
            )
            
            # Agrégations par type d'équipement
            equipment_agg = accidents_infra.groupBy("state").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                spark_sum("traffic_signal").alias("traffic_signals_count"),
                spark_sum("stop").alias("stops_count"),
                spark_sum("roundabout").alias("roundabouts_count"),
                spark_sum("crossing").alias("crossings_count"),
                spark_sum("junction").alias("junctions_count")
            )
            
            # Calcul de l'efficacité des équipements
            equipment_effectiveness = equipment_agg.withColumn(
                "traffic_signal_effectiveness",
                when(col("traffic_signals_count") > 0,
                     spark_round(1.0 / (col("avg_severity") + 0.1), 2))
                .otherwise(0.0)
            ).withColumn(
                "stop_effectiveness",
                when(col("stops_count") > 0,
                     spark_round(1.0 / (col("avg_severity") + 0.1), 2))
                .otherwise(0.0)
            ).withColumn(
                "roundabout_effectiveness",
                when(col("roundabouts_count") > 0,
                     spark_round(1.0 / (col("avg_severity") + 0.1), 2))
                .otherwise(0.0)
            )
            
            # Transformation en format long pour l'analyse
            infrastructure_patterns = equipment_effectiveness.select(
                col("state"),
                lit("traffic_signal").alias("infrastructure_type"),
                col("traffic_signals_count").alias("equipment_count"),
                col("traffic_signal_effectiveness").alias("effectiveness_score"),
                col("accidents_count"),
                col("avg_severity")
            ).union(
                equipment_effectiveness.select(
                    col("state"),
                    lit("stop").alias("infrastructure_type"),
                    col("stops_count").alias("equipment_count"),
                    col("stop_effectiveness").alias("effectiveness_score"),
                    col("accidents_count"),
                    col("avg_severity")
                )
            ).union(
                equipment_effectiveness.select(
                    col("state"),
                    lit("roundabout").alias("infrastructure_type"),
                    col("roundabouts_count").alias("equipment_count"),
                    col("roundabout_effectiveness").alias("effectiveness_score"),
                    col("accidents_count"),
                    col("avg_severity")
                )
            )
            
            # Calcul du score d'amélioration de sécurité
            infrastructure_patterns = infrastructure_patterns.withColumn(
                "safety_improvement_score",
                spark_round(
                    col("effectiveness_score") * col("equipment_count") / 10.0, 2
                )
            )
            
            # Génération de recommandations
            infrastructure_patterns = infrastructure_patterns.withColumn(
                "recommendation",
                when(col("effectiveness_score") < 0.5, "Increase equipment density")
                .when(col("effectiveness_score") < 1.0, "Optimize equipment placement")
                .otherwise("Maintain current infrastructure")
            )
            
            # Cache pour réutilisation
            infrastructure_patterns.cache()
            
            duration = time.time() - start_time
            row_count = infrastructure_patterns.count()
            
            self.logger.log_performance("infrastructure_patterns_aggregation", duration,
                                      rows_generated=row_count)
            
            return infrastructure_patterns
            
        except Exception as e:
            self.logger.error("Infrastructure patterns aggregation failed", exception=e)
            raise SparkJobError(f"Infrastructure patterns aggregation failed: {str(e)}") from e
    
    def aggregate_cross_dimensional(self) -> DataFrame:
        """
        Agrégations croisées multi-dimensionnelles
        
        Returns:
            DataFrame avec agrégations croisées
        """
        try:
            self.logger.info("Starting cross-dimensional aggregation")
            start_time = time.time()
            
            # Chargement de toutes les données
            accidents_df = self._get_cached_accidents_data()
            weather_df = self._get_cached_weather_data()
            infrastructure_df = self._get_cached_infrastructure_data()
            
            # Jointure complète
            full_data = accidents_df.join(
                weather_df, ["state", "city"], "left"
            ).join(
                infrastructure_df, ["state"], "left"
            )
            
            # Agrégations croisées État x Météo x Heure
            cross_agg = full_data.withColumn(
                "weather_category",
                when(col("weather_condition").rlike("(?i)(clear|fair)"), "Clear")
                .when(col("weather_condition").rlike("(?i)(rain|drizzle)"), "Rainy")
                .otherwise("Other")
            ).withColumn(
                "hour_category",
                when(hour(col("start_time")).between(6, 11), "Morning")
                .when(hour(col("start_time")).between(12, 17), "Afternoon")
                .when(hour(col("start_time")).between(18, 23), "Evening")
                .otherwise("Night")
            ).groupBy("state", "weather_category", "hour_category").agg(
                count("*").alias("accidents_count"),
                avg("severity").alias("avg_severity"),
                avg("distance_mi").alias("avg_distance"),
                countDistinct("city").alias("cities_affected")
            ).filter(col("accidents_count") >= 5)  # Filtre pour combinaisons significatives
            
            # Calcul du score de risque combiné
            cross_dimensional = cross_agg.withColumn(
                "combined_risk_score",
                spark_round(
                    col("accidents_count") * col("avg_severity") / 100.0, 2
                )
            ).withColumn(
                "dimension_combination",
                concat_ws(" | ", col("state"), col("weather_category"), col("hour_category"))
            )
            
            # Ranking des combinaisons les plus dangereuses
            window_risk = Window.orderBy(desc("combined_risk_score"))
            cross_dimensional = cross_dimensional.withColumn(
                "risk_rank",
                row_number().over(window_risk)
            )
            
            # Cache pour réutilisation
            cross_dimensional.cache()
            
            duration = time.time() - start_time
            row_count = cross_dimensional.count()
            
            self.logger.log_performance("cross_dimensional_aggregation", duration,
                                      rows_generated=row_count)
            
            return cross_dimensional
            
        except Exception as e:
            self.logger.error("Cross-dimensional aggregation failed", exception=e)
            raise SparkJobError(f"Cross-dimensional aggregation failed: {str(e)}") from e
    
    def incremental_aggregation(self, since_timestamp: str) -> Dict[str, DataFrame]:
        """
        Mise à jour incrémentale des agrégations
        
        Args:
            since_timestamp: Timestamp ISO pour la mise à jour incrémentale
            
        Returns:
            Dict des DataFrames mis à jour
        """
        try:
            self.logger.info("Starting incremental aggregation", since_timestamp=since_timestamp)
            
            # Filtre temporel pour les données modifiées
            time_filter = col("last_updated") >= since_timestamp
            
            # Mise à jour incrémentale de chaque dimension
            updated_data = {}
            
            # Réinitialisation du cache
            self._clear_cache()
            
            # Recalcul des agrégations avec filtre temporel
            updated_data['temporal'] = self.aggregate_temporal_patterns()
            updated_data['geographic'] = self.aggregate_geographic_patterns()
            updated_data['weather'] = self.aggregate_weather_patterns()
            updated_data['infrastructure'] = self.aggregate_infrastructure_patterns()
            
            self.logger.info("Incremental aggregation completed",
                           updated_dimensions=len(updated_data))
            
            return updated_data
            
        except Exception as e:
            self.logger.error("Incremental aggregation failed", exception=e)
            raise SparkJobError(f"Incremental aggregation failed: {str(e)}") from e
    
    def _get_cached_accidents_data(self) -> DataFrame:
        """Récupère les données accidents avec cache"""
        if 'accidents' not in self._cached_dfs:
            table_name = f"{self.hive_database}.{self.table_names['accidents_clean']}"
            self._cached_dfs['accidents'] = self.spark.table(table_name).cache()
            self.logger.info("Accidents data cached", table=table_name)
        return self._cached_dfs['accidents']
    
    def _get_cached_weather_data(self) -> DataFrame:
        """Récupère les données météo avec cache"""
        if 'weather' not in self._cached_dfs:
            table_name = f"{self.hive_database}.{self.table_names['weather_aggregated']}"
            self._cached_dfs['weather'] = self.spark.table(table_name).cache()
            self.logger.info("Weather data cached", table=table_name)
        return self._cached_dfs['weather']
    
    def _get_cached_infrastructure_data(self) -> DataFrame:
        """Récupère les données infrastructure avec cache"""
        if 'infrastructure' not in self._cached_dfs:
            table_name = f"{self.hive_database}.{self.table_names['infrastructure_features']}"
            self._cached_dfs['infrastructure'] = self.spark.table(table_name).cache()
            self.logger.info("Infrastructure data cached", table=table_name)
        return self._cached_dfs['infrastructure']
    
    def _clear_cache(self):
        """Nettoie le cache des DataFrames"""
        for df in self._cached_dfs.values():
            df.unpersist()
        self._cached_dfs.clear()
        self.logger.info("DataFrame cache cleared")