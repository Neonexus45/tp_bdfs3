"""
Composant AggregationEngine pour les agrégations avancées des données US-Accidents
"""

from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    stddev, variance, percentile_approx, collect_list, collect_set,
    when, coalesce, lit, round as spark_round,
    year, month, dayofweek, hour, date_format,
    row_number, rank, dense_rank, lag, lead,
    first, last, ntile, percent_rank,
    array, struct, explode,
    regexp_extract, split, size, sort_array, countDistinct
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import DataValidationError


class AggregationEngine:
    """
    Moteur d'agrégations avancées pour l'analyse des données US-Accidents
    
    Fonctionnalités:
    - Agrégations temporelles (par heure, jour, mois, saison)
    - Agrégations géographiques (par état, région, zone urbaine/rurale)
    - Agrégations météorologiques (par condition, température, visibilité)
    - Agrégations d'infrastructure (par équipement, score de sécurité)
    - Analyses de tendances et patterns
    - Calculs de métriques avancées (percentiles, corrélations)
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le moteur d'agrégations"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("aggregation_engine", self.config_manager)
        
        # Configuration des agrégations
        self.aggregation_config = {
            'temporal_granularities': ['hour', 'day', 'month', 'season', 'year'],
            'geographic_levels': ['state', 'region', 'urban_rural'],
            'weather_categories': ['Clear', 'Rain', 'Snow', 'Fog', 'Cloudy', 'Other'],
            'severity_levels': [1, 2, 3, 4],
            'percentiles': [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        }
        
        # Métriques d'agrégation
        self.aggregation_metrics = {
            'aggregations_created': 0,
            'temporal_aggregations': 0,
            'geographic_aggregations': 0,
            'weather_aggregations': 0,
            'infrastructure_aggregations': 0,
            'advanced_analytics': 0,
            'total_execution_time': 0.0
        }
    
    def create_all_aggregations(self, df: DataFrame) -> Dict[str, DataFrame]:
        """
        Crée toutes les agrégations disponibles
        
        Args:
            df: DataFrame enrichi avec features
            
        Returns:
            Dict contenant tous les DataFrames d'agrégation
        """
        try:
            import time
            start_time = time.time()
            
            self.logger.info("Starting comprehensive aggregation creation")
            
            aggregations = {}
            
            # Agrégations temporelles
            temporal_aggs = self._create_temporal_aggregations(df)
            aggregations.update(temporal_aggs)
            
            # Agrégations géographiques
            geographic_aggs = self._create_geographic_aggregations(df)
            aggregations.update(geographic_aggs)
            
            # Agrégations météorologiques
            weather_aggs = self._create_weather_aggregations(df)
            aggregations.update(weather_aggs)
            
            # Agrégations d'infrastructure
            infrastructure_aggs = self._create_infrastructure_aggregations(df)
            aggregations.update(infrastructure_aggs)
            
            # Analyses avancées
            advanced_aggs = self._create_advanced_analytics(df)
            aggregations.update(advanced_aggs)
            
            # Calcul des métriques finales
            self.aggregation_metrics['aggregations_created'] = len(aggregations)
            self.aggregation_metrics['total_execution_time'] = time.time() - start_time
            
            self.logger.log_performance(
                operation="all_aggregations_complete",
                duration_seconds=self.aggregation_metrics['total_execution_time'],
                aggregations_created=self.aggregation_metrics['aggregations_created']
            )
            
            return aggregations
            
        except Exception as e:
            self.logger.error("Failed to create aggregations", exception=e)
            raise DataValidationError(f"Aggregation creation failed: {str(e)}")
    
    def _create_temporal_aggregations(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Crée les agrégations temporelles"""
        try:
            temporal_aggs = {}
            
            # Agrégation par heure
            if 'accident_hour' in df.columns:
                hourly_agg = df.groupBy('accident_hour') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        countDistinct('State').alias('states_affected'),
                        spark_sum(when(col('is_rush_hour') == True, 1).otherwise(0)).alias('rush_hour_accidents')
                    ) \
                    .orderBy('accident_hour')
                
                temporal_aggs['hourly_patterns'] = hourly_agg
                self.aggregation_metrics['temporal_aggregations'] += 1
            
            # Agrégation par jour de la semaine
            if 'accident_day_of_week' in df.columns:
                daily_agg = df.groupBy('accident_day_of_week') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        spark_sum(when(col('is_weekend') == True, 1).otherwise(0)).alias('weekend_accidents'),
                        countDistinct('weather_category').alias('weather_variety')
                    ) \
                    .orderBy('accident_day_of_week')
                
                temporal_aggs['daily_patterns'] = daily_agg
                self.aggregation_metrics['temporal_aggregations'] += 1
            
            # Agrégation par mois
            if 'accident_month' in df.columns:
                monthly_agg = df.groupBy('accident_month') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('temperature_fahrenheit').alias('avg_temperature'),
                        countDistinct('weather_category').alias('weather_conditions')
                    ) \
                    .orderBy('accident_month')
                
                temporal_aggs['monthly_patterns'] = monthly_agg
                self.aggregation_metrics['temporal_aggregations'] += 1
            
            # Agrégation par saison
            if 'accident_season' in df.columns:
                seasonal_agg = df.groupBy('accident_season') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('temperature_fahrenheit').alias('avg_temperature'),
                        avg('visibility_miles').alias('avg_visibility'),
                        spark_max('weather_severity_score').alias('max_weather_severity')
                    )
                
                temporal_aggs['seasonal_patterns'] = seasonal_agg
                self.aggregation_metrics['temporal_aggregations'] += 1
            
            # Tendances temporelles complexes
            if all(col in df.columns for col in ['accident_hour', 'accident_day_of_week']):
                time_matrix = df.groupBy('accident_hour', 'accident_day_of_week') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity')
                    ) \
                    .orderBy('accident_day_of_week', 'accident_hour')
                
                temporal_aggs['hourly_daily_matrix'] = time_matrix
                self.aggregation_metrics['temporal_aggregations'] += 1
            
            self.logger.info("Temporal aggregations created", 
                           count=self.aggregation_metrics['temporal_aggregations'])
            
            return temporal_aggs
            
        except Exception as e:
            self.logger.error("Failed to create temporal aggregations", exception=e)
            return {}
    
    def _create_geographic_aggregations(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Crée les agrégations géographiques"""
        try:
            geographic_aggs = {}
            
            # Agrégation par état
            if 'State' in df.columns:
                state_agg = df.groupBy('State') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('distance_to_city_center').alias('avg_distance_to_city'),
                        countDistinct('City').alias('cities_affected'),
                        spark_sum('infrastructure_count').alias('total_infrastructure'),
                        avg('safety_equipment_score').alias('avg_safety_score')
                    ) \
                    .orderBy(col('accident_count').desc())
                
                geographic_aggs['state_summary'] = state_agg
                self.aggregation_metrics['geographic_aggregations'] += 1
            
            # Agrégation par région
            if 'state_region' in df.columns:
                region_agg = df.groupBy('state_region') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        countDistinct('State').alias('states_in_region'),
                        avg('temperature_fahrenheit').alias('avg_temperature'),
                        countDistinct('weather_category').alias('weather_variety')
                    )
                
                geographic_aggs['regional_summary'] = region_agg
                self.aggregation_metrics['geographic_aggregations'] += 1
            
            # Agrégation urbain/rural
            if 'urban_rural_classification' in df.columns:
                urban_rural_agg = df.groupBy('urban_rural_classification') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('distance_to_city_center').alias('avg_distance_to_city'),
                        avg('infrastructure_count').alias('avg_infrastructure'),
                        avg('safety_equipment_score').alias('avg_safety_score')
                    )
                
                geographic_aggs['urban_rural_analysis'] = urban_rural_agg
                self.aggregation_metrics['geographic_aggregations'] += 1
            
            # Hotspots géographiques (top villes par accidents)
            if all(col in df.columns for col in ['State', 'City']):
                city_hotspots = df.groupBy('State', 'City') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('infrastructure_count').alias('avg_infrastructure')
                    ) \
                    .filter(col('accident_count') >= 100) \
                    .orderBy(col('accident_count').desc()) \
                    .limit(50)
                
                geographic_aggs['city_hotspots'] = city_hotspots
                self.aggregation_metrics['geographic_aggregations'] += 1
            
            # Analyse par densité de population
            if 'population_density_category' in df.columns:
                density_agg = df.groupBy('population_density_category') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('infrastructure_count').alias('avg_infrastructure'),
                        countDistinct('traffic_control_type').alias('traffic_control_variety')
                    )
                
                geographic_aggs['population_density_analysis'] = density_agg
                self.aggregation_metrics['geographic_aggregations'] += 1
            
            self.logger.info("Geographic aggregations created", 
                           count=self.aggregation_metrics['geographic_aggregations'])
            
            return geographic_aggs
            
        except Exception as e:
            self.logger.error("Failed to create geographic aggregations", exception=e)
            return {}
    
    def _create_weather_aggregations(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Crée les agrégations météorologiques"""
        try:
            weather_aggs = {}
            
            # Agrégation par catégorie météo
            if 'weather_category' in df.columns:
                weather_category_agg = df.groupBy('weather_category') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('temperature_fahrenheit').alias('avg_temperature'),
                        avg('humidity_percent').alias('avg_humidity'),
                        avg('visibility_miles').alias('avg_visibility'),
                        avg('wind_speed_mph').alias('avg_wind_speed')
                    ) \
                    .orderBy(col('accident_count').desc())
                
                weather_aggs['weather_category_impact'] = weather_category_agg
                self.aggregation_metrics['weather_aggregations'] += 1
            
            # Agrégation par score de sévérité météo
            if 'weather_severity_score' in df.columns:
                weather_severity_agg = df.groupBy('weather_severity_score') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_accident_severity'),
                        countDistinct('State').alias('states_affected')
                    ) \
                    .orderBy('weather_severity_score')
                
                weather_aggs['weather_severity_correlation'] = weather_severity_agg
                self.aggregation_metrics['weather_aggregations'] += 1
            
            # Agrégation par catégorie de température
            if 'temperature_category' in df.columns:
                temp_category_agg = df.groupBy('temperature_category') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('temperature_fahrenheit').alias('avg_temperature'),
                        spark_min('temperature_fahrenheit').alias('min_temperature'),
                        spark_max('temperature_fahrenheit').alias('max_temperature')
                    )
                
                weather_aggs['temperature_impact'] = temp_category_agg
                self.aggregation_metrics['weather_aggregations'] += 1
            
            # Agrégation par catégorie de visibilité
            if 'visibility_category' in df.columns:
                visibility_agg = df.groupBy('visibility_category') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('visibility_miles').alias('avg_visibility'),
                        countDistinct('weather_category').alias('weather_conditions')
                    )
                
                weather_aggs['visibility_impact'] = visibility_agg
                self.aggregation_metrics['weather_aggregations'] += 1
            
            # Analyse croisée météo-temporelle
            if all(col in df.columns for col in ['weather_category', 'accident_season']):
                weather_seasonal = df.groupBy('weather_category', 'accident_season') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('temperature_fahrenheit').alias('avg_temperature')
                    ) \
                    .orderBy('accident_season', col('accident_count').desc())
                
                weather_aggs['weather_seasonal_patterns'] = weather_seasonal
                self.aggregation_metrics['weather_aggregations'] += 1
            
            self.logger.info("Weather aggregations created", 
                           count=self.aggregation_metrics['weather_aggregations'])
            
            return weather_aggs
            
        except Exception as e:
            self.logger.error("Failed to create weather aggregations", exception=e)
            return {}
    
    def _create_infrastructure_aggregations(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Crée les agrégations d'infrastructure"""
        try:
            infrastructure_aggs = {}
            
            # Agrégation par type de contrôle du trafic
            if 'traffic_control_type' in df.columns:
                traffic_control_agg = df.groupBy('traffic_control_type') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('safety_equipment_score').alias('avg_safety_score'),
                        countDistinct('State').alias('states_with_control')
                    )
                
                infrastructure_aggs['traffic_control_effectiveness'] = traffic_control_agg
                self.aggregation_metrics['infrastructure_aggregations'] += 1
            
            # Agrégation par score de sécurité
            if 'safety_equipment_score' in df.columns:
                safety_score_agg = df.groupBy('safety_equipment_score') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        countDistinct('traffic_control_type').alias('control_types')
                    ) \
                    .orderBy('safety_equipment_score')
                
                infrastructure_aggs['safety_score_impact'] = safety_score_agg
                self.aggregation_metrics['infrastructure_aggregations'] += 1
            
            # Agrégation par nombre d'équipements
            if 'infrastructure_count' in df.columns:
                infra_count_agg = df.groupBy('infrastructure_count') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        avg('safety_equipment_score').alias('avg_safety_score')
                    ) \
                    .orderBy('infrastructure_count')
                
                infrastructure_aggs['infrastructure_density_impact'] = infra_count_agg
                self.aggregation_metrics['infrastructure_aggregations'] += 1
            
            # Analyse des équipements individuels
            infrastructure_columns = [
                'Traffic_Signal', 'Stop', 'Traffic_Calming', 'Railway', 'Crossing',
                'Junction', 'Roundabout', 'Give_Way', 'Station'
            ]
            
            available_infra_cols = [col for col in infrastructure_columns if col in df.columns]
            
            if available_infra_cols:
                # Agrégation pour chaque type d'équipement
                equipment_analysis = []
                
                for equipment in available_infra_cols:
                    equipment_stats = df.groupBy(equipment) \
                        .agg(
                            count('*').alias('accident_count'),
                            avg('Severity').alias('avg_severity')
                        ) \
                        .withColumn('equipment_type', lit(equipment))
                    
                    equipment_analysis.append(equipment_stats)
                
                if equipment_analysis:
                    # Union de toutes les analyses d'équipements
                    combined_equipment = equipment_analysis[0]
                    for eq_df in equipment_analysis[1:]:
                        combined_equipment = combined_equipment.union(eq_df)
                    
                    infrastructure_aggs['individual_equipment_analysis'] = combined_equipment
                    self.aggregation_metrics['infrastructure_aggregations'] += 1
            
            self.logger.info("Infrastructure aggregations created", 
                           count=self.aggregation_metrics['infrastructure_aggregations'])
            
            return infrastructure_aggs
            
        except Exception as e:
            self.logger.error("Failed to create infrastructure aggregations", exception=e)
            return {}
    
    def _create_advanced_analytics(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Crée les analyses avancées et métriques complexes"""
        try:
            advanced_aggs = {}
            
            # Analyse des percentiles de sévérité par état
            if all(col in df.columns for col in ['State', 'Severity']):
                severity_percentiles = df.groupBy('State') \
                    .agg(
                        count('*').alias('accident_count'),
                        *[percentile_approx('Severity', p).alias(f'severity_p{int(p*100)}') 
                          for p in self.aggregation_config['percentiles']]
                    ) \
                    .filter(col('accident_count') >= 100)
                
                advanced_aggs['severity_percentiles_by_state'] = severity_percentiles
                self.aggregation_metrics['advanced_analytics'] += 1
            
            # Ranking des états par différentes métriques
            if 'State' in df.columns:
                state_rankings = df.groupBy('State') \
                    .agg(
                        count('*').alias('total_accidents'),
                        avg('Severity').alias('avg_severity'),
                        avg('safety_equipment_score').alias('avg_safety_score')
                    ) \
                    .withColumn('accident_rank', 
                               dense_rank().over(Window.orderBy(col('total_accidents').desc()))) \
                    .withColumn('severity_rank', 
                               dense_rank().over(Window.orderBy(col('avg_severity').desc()))) \
                    .withColumn('safety_rank', 
                               dense_rank().over(Window.orderBy(col('avg_safety_score').desc())))
                
                advanced_aggs['state_rankings'] = state_rankings
                self.aggregation_metrics['advanced_analytics'] += 1
            
            # Analyse des corrélations météo-sévérité
            if all(col in df.columns for col in ['weather_severity_score', 'Severity', 'temperature_fahrenheit']):
                correlation_analysis = df.groupBy('weather_severity_score') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_accident_severity'),
                        avg('temperature_fahrenheit').alias('avg_temperature'),
                        stddev('Severity').alias('severity_stddev'),
                        spark_min('Severity').alias('min_severity'),
                        spark_max('Severity').alias('max_severity')
                    ) \
                    .orderBy('weather_severity_score')
                
                advanced_aggs['weather_severity_correlation'] = correlation_analysis
                self.aggregation_metrics['advanced_analytics'] += 1
            
            # Analyse des patterns temporels complexes
            if all(col in df.columns for col in ['accident_hour', 'is_weekend', 'weather_category']):
                complex_temporal = df.groupBy('accident_hour', 'is_weekend', 'weather_category') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity')
                    ) \
                    .filter(col('accident_count') >= 10) \
                    .orderBy('accident_hour', 'is_weekend', col('accident_count').desc())
                
                advanced_aggs['complex_temporal_patterns'] = complex_temporal
                self.aggregation_metrics['advanced_analytics'] += 1
            
            # Analyse des outliers géographiques
            if all(col in df.columns for col in ['State', 'City', 'Severity']):
                geographic_outliers = df.groupBy('State', 'City') \
                    .agg(
                        count('*').alias('accident_count'),
                        avg('Severity').alias('avg_severity'),
                        spark_max('Severity').alias('max_severity')
                    ) \
                    .filter(
                        (col('accident_count') >= 50) & 
                        (col('avg_severity') >= 3.0)
                    ) \
                    .orderBy(col('avg_severity').desc(), col('accident_count').desc())
                
                advanced_aggs['high_severity_locations'] = geographic_outliers
                self.aggregation_metrics['advanced_analytics'] += 1
            
            self.logger.info("Advanced analytics created", 
                           count=self.aggregation_metrics['advanced_analytics'])
            
            return advanced_aggs
            
        except Exception as e:
            self.logger.error("Failed to create advanced analytics", exception=e)
            return {}
    
    def get_aggregation_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques d'agrégation"""
        return self.aggregation_metrics.copy()
    
    def get_aggregation_summary(self) -> Dict[str, Any]:
        """Génère un résumé des agrégations créées"""
        try:
            return {
                'timestamp': self.logger._get_hostname(),
                'aggregation_summary': {
                    'total_aggregations': self.aggregation_metrics['aggregations_created'],
                    'execution_time_seconds': self.aggregation_metrics['total_execution_time']
                },
                'aggregation_categories': {
                    'temporal_aggregations': self.aggregation_metrics['temporal_aggregations'],
                    'geographic_aggregations': self.aggregation_metrics['geographic_aggregations'],
                    'weather_aggregations': self.aggregation_metrics['weather_aggregations'],
                    'infrastructure_aggregations': self.aggregation_metrics['infrastructure_aggregations'],
                    'advanced_analytics': self.aggregation_metrics['advanced_analytics']
                },
                'configuration': self.aggregation_config
            }
            
        except Exception as e:
            self.logger.error("Failed to generate aggregation summary", exception=e)
            return {'error': str(e)}