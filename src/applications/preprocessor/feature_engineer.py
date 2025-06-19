"""
Composant FeatureEngineer pour le feature engineering avancé des données US-Accidents
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, hour, dayofweek, month, date_format, lit,
    regexp_extract, regexp_replace, lower, upper, trim,
    sqrt, pow, sin, cos, radians, asin, atan2,
    coalesce, greatest, least, array, size,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    dense_rank, row_number, lag, lead,
    unix_timestamp, from_unixtime, datediff, date_add,
    split, explode, collect_list, collect_set,
    udf, broadcast
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, BooleanType
from pyspark.sql.window import Window
import math

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import DataValidationError


class FeatureEngineer:
    """
    Composant de feature engineering avancé pour les données US-Accidents
    
    Fonctionnalités:
    - Features temporelles: heure, jour, mois, saison, weekend, rush hour
    - Features météorologiques: catégorisation, scores de sévérité
    - Features géographiques: distance centre-ville, classification urbain/rural
    - Features infrastructure: agrégation des 13 variables booléennes
    - Encoding des variables catégorielles
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le composant FeatureEngineer"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("feature_engineer", self.config_manager)
        
        # Configuration des features
        self.rush_hours = [(7, 9), (17, 19)]  # Heures de pointe
        self.weekend_days = [1, 7]  # Dimanche et Samedi
        
        # Mapping des saisons
        self.season_mapping = {
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall'
        }
        
        # Mapping des régions US
        self.state_regions = {
            'Northeast': ['CT', 'ME', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'],
            'Southeast': ['AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV'],
            'Midwest': ['IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI'],
            'Southwest': ['AZ', 'NM', 'OK', 'TX'],
            'West': ['AK', 'CA', 'CO', 'HI', 'ID', 'MT', 'NV', 'OR', 'UT', 'WA', 'WY'],
            'Other': ['DC']
        }
        
        # Colonnes d'infrastructure
        self.infrastructure_columns = [
            'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
            'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming',
            'Traffic_Signal', 'Turning_Loop'
        ]
        
        # Pondération des équipements de sécurité
        self.safety_weights = {
            'Traffic_Signal': 3, 'Stop': 3, 'Traffic_Calming': 2,
            'Railway': 2, 'Crossing': 2, 'Junction': 1,
            'Roundabout': 1, 'Give_Way': 1, 'Station': 1,
            'Amenity': 0, 'Bump': 0, 'No_Exit': 0, 'Turning_Loop': 0
        }
        
        # Centres des principales villes US (lat, lng)
        self.major_cities = {
            'CA': (34.0522, -118.2437),  # Los Angeles
            'NY': (40.7128, -74.0060),   # New York
            'TX': (29.7604, -95.3698),   # Houston
            'FL': (25.7617, -80.1918),   # Miami
            'IL': (41.8781, -87.6298),   # Chicago
            'PA': (39.9526, -75.1652),   # Philadelphia
            'OH': (39.9612, -82.9988),   # Columbus
            'GA': (33.7490, -84.3880),   # Atlanta
            'NC': (35.2271, -80.8431),   # Charlotte
            'MI': (42.3314, -83.0458),   # Detroit
        }
        
        # Métriques de feature engineering
        self.feature_metrics = {
            'features_created': 0,
            'temporal_features': 0,
            'weather_features': 0,
            'geographic_features': 0,
            'infrastructure_features': 0,
            'categorical_encodings': 0,
            'feature_engineering_score': 0.0
        }
    
    def create_features(self, df: DataFrame) -> DataFrame:
        """
        Crée toutes les features avancées
        
        Args:
            df: DataFrame nettoyé
            
        Returns:
            DataFrame enrichi avec les nouvelles features
        """
        try:
            self.logger.info("Starting feature engineering", 
                           input_columns=len(df.columns))
            
            enriched_df = df
            
            # Étape 1: Features temporelles
            enriched_df = self._create_temporal_features(enriched_df)
            
            # Étape 2: Features météorologiques
            enriched_df = self._create_weather_features(enriched_df)
            
            # Étape 3: Features géographiques
            enriched_df = self._create_geographic_features(enriched_df)
            
            # Étape 4: Features d'infrastructure
            enriched_df = self._create_infrastructure_features(enriched_df)
            
            # Étape 5: Encoding des variables catégorielles
            enriched_df = self._encode_categorical_variables(enriched_df)
            
            # Calcul des métriques finales
            self._calculate_feature_metrics(enriched_df, df)
            
            self.logger.log_performance(
                operation="feature_engineering_complete",
                duration_seconds=0,
                input_columns=len(df.columns),
                output_columns=len(enriched_df.columns),
                features_created=self.feature_metrics['features_created']
            )
            
            return enriched_df
            
        except Exception as e:
            self.logger.error("Feature engineering failed", exception=e)
            raise DataValidationError(
                message=f"Feature engineering failed: {str(e)}",
                validation_type="feature_engineering",
                dataset="us_accidents"
            )
    
    def _create_temporal_features(self, df: DataFrame) -> DataFrame:
        """Crée les features temporelles depuis Start_Time"""
        try:
            if 'Start_Time' not in df.columns:
                self.logger.warning("Start_Time column not found, skipping temporal features")
                return df
            
            temporal_df = df
            features_added = 0
            
            # Extraction de l'heure (0-23)
            temporal_df = temporal_df.withColumn('accident_hour', hour(col('Start_Time')))
            features_added += 1
            
            # Jour de la semaine (1=Dimanche, 7=Samedi)
            temporal_df = temporal_df.withColumn('accident_day_of_week', dayofweek(col('Start_Time')))
            features_added += 1
            
            # Mois (1-12)
            temporal_df = temporal_df.withColumn('accident_month', month(col('Start_Time')))
            features_added += 1
            
            # Saison
            season_when = when(col('accident_month') == 12, 'Winter')
            for month_num, season in self.season_mapping.items():
                if month_num != 12:  # Skip the first one already handled
                    season_when = season_when.when(col('accident_month') == month_num, season)
            temporal_df = temporal_df.withColumn('accident_season', season_when.otherwise('Unknown'))
            features_added += 1
            
            # Weekend (booléen)
            temporal_df = temporal_df.withColumn(
                'is_weekend',
                col('accident_day_of_week').isin(self.weekend_days)
            )
            features_added += 1
            
            # Heure de pointe (booléen)
            rush_hour_condition = lit(False)
            for start_hour, end_hour in self.rush_hours:
                rush_hour_condition = rush_hour_condition | (
                    (col('accident_hour') >= start_hour) & (col('accident_hour') < end_hour)
                )
            temporal_df = temporal_df.withColumn('is_rush_hour', rush_hour_condition)
            features_added += 1
            
            self.feature_metrics['temporal_features'] = features_added
            
            self.logger.info("Temporal features created", 
                           features_count=features_added)
            
            return temporal_df
            
        except Exception as e:
            self.logger.error("Temporal feature creation failed", exception=e)
            return df
    
    def _create_weather_features(self, df: DataFrame) -> DataFrame:
        """Crée les features météorologiques"""
        try:
            weather_df = df
            features_added = 0
            
            # Catégorisation des conditions météo
            if 'Weather_Condition' in weather_df.columns:
                weather_df = weather_df.withColumn(
                    'weather_category',
                    when(lower(col('Weather_Condition')).contains('clear'), 'Clear')
                    .when(lower(col('Weather_Condition')).contains('rain'), 'Rain')
                    .when(lower(col('Weather_Condition')).contains('snow'), 'Snow')
                    .when(lower(col('Weather_Condition')).contains('fog'), 'Fog')
                    .when(lower(col('Weather_Condition')).contains('cloud'), 'Cloudy')
                    .when(lower(col('Weather_Condition')).contains('wind'), 'Windy')
                    .otherwise('Other')
                )
                features_added += 1
            
            # Score de sévérité météorologique (0-10)
            if 'Weather_Condition' in weather_df.columns:
                weather_df = weather_df.withColumn(
                    'weather_severity_score',
                    when(col('weather_category') == 'Clear', 1)
                    .when(col('weather_category') == 'Cloudy', 3)
                    .when(col('weather_category') == 'Rain', 6)
                    .when(col('weather_category') == 'Fog', 7)
                    .when(col('weather_category') == 'Snow', 8)
                    .when(col('weather_category') == 'Windy', 5)
                    .otherwise(4)
                )
                features_added += 1
            
            # Catégorisation de la visibilité
            if 'visibility_miles' in weather_df.columns:
                weather_df = weather_df.withColumn(
                    'visibility_category',
                    when(col('visibility_miles') >= 10, 'Good')
                    .when(col('visibility_miles') >= 5, 'Fair')
                    .when(col('visibility_miles') >= 1, 'Poor')
                    .otherwise('Very Poor')
                )
                features_added += 1
            
            # Catégorisation de la température
            if 'temperature_fahrenheit' in weather_df.columns:
                weather_df = weather_df.withColumn(
                    'temperature_category',
                    when(col('temperature_fahrenheit') <= 32, 'Cold')
                    .when(col('temperature_fahrenheit') <= 70, 'Mild')
                    .when(col('temperature_fahrenheit') <= 90, 'Warm')
                    .otherwise('Hot')
                )
                features_added += 1
            
            self.feature_metrics['weather_features'] = features_added
            
            self.logger.info("Weather features created", 
                           features_count=features_added)
            
            return weather_df
            
        except Exception as e:
            self.logger.error("Weather feature creation failed", exception=e)
            return df
    
    def _create_geographic_features(self, df: DataFrame) -> DataFrame:
        """Crée les features géographiques"""
        try:
            geo_df = df
            features_added = 0
            
            # Distance au centre-ville le plus proche
            if 'Start_Lat' in geo_df.columns and 'Start_Lng' in geo_df.columns and 'State' in geo_df.columns:
                geo_df = self._add_distance_to_city_center(geo_df)
                features_added += 1
            
            # Classification urbain/rural basée sur la distance
            if 'distance_to_city_center' in geo_df.columns:
                geo_df = geo_df.withColumn(
                    'urban_rural_classification',
                    when(col('distance_to_city_center') <= 10, 'Urban')
                    .when(col('distance_to_city_center') <= 50, 'Suburban')
                    .otherwise('Rural')
                )
                features_added += 1
            
            # Région géographique
            if 'State' in geo_df.columns:
                region_when = when(col('State').isin(self.state_regions['Northeast']), 'Northeast')
                for region, states in self.state_regions.items():
                    if region != 'Northeast':  # Skip the first one already handled
                        region_when = region_when.when(col('State').isin(states), region)
                geo_df = geo_df.withColumn('state_region', region_when.otherwise('Unknown'))
                features_added += 1
            
            # Catégorie de densité de population (approximative)
            if 'urban_rural_classification' in geo_df.columns:
                geo_df = geo_df.withColumn(
                    'population_density_category',
                    when(col('urban_rural_classification') == 'Urban', 'High')
                    .when(col('urban_rural_classification') == 'Suburban', 'Medium')
                    .otherwise('Low')
                )
                features_added += 1
            
            self.feature_metrics['geographic_features'] = features_added
            
            self.logger.info("Geographic features created", 
                           features_count=features_added)
            
            return geo_df
            
        except Exception as e:
            self.logger.error("Geographic feature creation failed", exception=e)
            return df
    
    def _add_distance_to_city_center(self, df: DataFrame) -> DataFrame:
        """Ajoute la distance au centre-ville le plus proche"""
        try:
            # UDF pour calculer la distance haversine
            def haversine_distance(lat1, lon1, lat2, lon2):
                if any(x is None for x in [lat1, lon1, lat2, lon2]):
                    return None
                
                # Conversion en radians
                lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
                
                # Formule haversine
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
                c = 2 * math.asin(math.sqrt(a))
                
                # Rayon de la Terre en miles
                r = 3956
                return c * r
            
            haversine_udf = udf(haversine_distance, DoubleType())
            
            # Ajout de la distance pour chaque état avec centre-ville connu
            distance_df = df
            
            for state, (city_lat, city_lng) in self.major_cities.items():
                distance_df = distance_df.withColumn(
                    f'dist_to_{state}',
                    when(col('State') == state,
                         haversine_udf(col('Start_Lat'), col('Start_Lng'), 
                                     lit(city_lat), lit(city_lng)))
                    .otherwise(lit(None))
                )
            
            # Calcul de la distance minimale
            distance_columns = [f'dist_to_{state}' for state in self.major_cities.keys()]
            distance_df = distance_df.withColumn(
                'distance_to_city_center',
                coalesce(*[col(c) for c in distance_columns])
            )
            
            # Suppression des colonnes temporaires
            for col_name in distance_columns:
                distance_df = distance_df.drop(col_name)
            
            return distance_df
            
        except Exception as e:
            self.logger.error("Distance calculation failed", exception=e)
            return df
    
    def _create_infrastructure_features(self, df: DataFrame) -> DataFrame:
        """Crée les features d'infrastructure"""
        try:
            infra_df = df
            features_added = 0
            
            # Comptage total des équipements d'infrastructure
            available_infra_cols = [col_name for col_name in self.infrastructure_columns 
                                  if col_name in infra_df.columns]
            
            if available_infra_cols:
                # Conversion des booléens en entiers pour la somme
                sum_expr = spark_sum(
                    *[when(col(c) == True, 1).otherwise(0) for c in available_infra_cols]
                )
                infra_df = infra_df.withColumn('infrastructure_count', sum_expr)
                features_added += 1
                
                # Score de sécurité pondéré
                safety_score_expr = spark_sum(
                    *[when(col(c) == True, self.safety_weights.get(c, 0)).otherwise(0) 
                      for c in available_infra_cols if c in self.safety_weights]
                )
                infra_df = infra_df.withColumn('safety_equipment_score', safety_score_expr)
                features_added += 1
                
                # Type de contrôle du trafic
                infra_df = infra_df.withColumn(
                    'traffic_control_type',
                    when(col('Traffic_Signal') == True, 'Signal')
                    .when(col('Stop') == True, 'Stop')
                    .when((col('Traffic_Signal') == True) & (col('Stop') == True), 'Multiple')
                    .otherwise('None')
                )
                features_added += 1
            
            self.feature_metrics['infrastructure_features'] = features_added
            
            self.logger.info("Infrastructure features created", 
                           features_count=features_added)
            
            return infra_df
            
        except Exception as e:
            self.logger.error("Infrastructure feature creation failed", exception=e)
            return df
    
    def _encode_categorical_variables(self, df: DataFrame) -> DataFrame:
        """Encode les variables catégorielles"""
        try:
            encoded_df = df
            encodings_added = 0
            
            # Variables catégorielles à encoder
            categorical_columns = {
                'Source': 'source_encoded',
                'weather_category': 'weather_category_encoded',
                'state_region': 'state_region_encoded',
                'urban_rural_classification': 'urban_rural_encoded',
                'traffic_control_type': 'traffic_control_encoded'
            }
            
            for original_col, encoded_col in categorical_columns.items():
                if original_col in encoded_df.columns:
                    # Encodage simple par index (peut être amélioré avec StringIndexer)
                    distinct_values = [row[original_col] for row in 
                                     encoded_df.select(original_col).distinct().collect()]
                    
                    if distinct_values:
                        encoding_when = when(col(original_col) == distinct_values[0], 0)
                        for i, value in enumerate(distinct_values[1:], 1):
                            encoding_when = encoding_when.when(col(original_col) == value, i)
                        encoded_df = encoded_df.withColumn(encoded_col, encoding_when.otherwise(-1))
                    encodings_added += 1
            
            self.feature_metrics['categorical_encodings'] = encodings_added
            
            self.logger.info("Categorical variables encoded", 
                           encodings_count=encodings_added)
            
            return encoded_df
            
        except Exception as e:
            self.logger.error("Categorical encoding failed", exception=e)
            return df
    
    def _calculate_feature_metrics(self, enriched_df: DataFrame, original_df: DataFrame):
        """Calcule les métriques de feature engineering"""
        try:
            original_columns = len(original_df.columns)
            enriched_columns = len(enriched_df.columns)
            
            self.feature_metrics['features_created'] = enriched_columns - original_columns
            
            # Score de feature engineering basé sur le nombre et la qualité des features
            temporal_score = min(self.feature_metrics['temporal_features'] * 10, 60)
            weather_score = min(self.feature_metrics['weather_features'] * 15, 60)
            geographic_score = min(self.feature_metrics['geographic_features'] * 15, 60)
            infrastructure_score = min(self.feature_metrics['infrastructure_features'] * 20, 60)
            encoding_score = min(self.feature_metrics['categorical_encodings'] * 10, 40)
            
            total_score = (temporal_score + weather_score + geographic_score + 
                          infrastructure_score + encoding_score) / 5
            
            self.feature_metrics['feature_engineering_score'] = round(total_score, 2)
            
        except Exception as e:
            self.logger.error("Feature metrics calculation failed", exception=e)
            self.feature_metrics['feature_engineering_score'] = 0.0
    
    def get_feature_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques de feature engineering"""
        return self.feature_metrics.copy()
    
    def get_feature_report(self) -> Dict[str, Any]:
        """Génère un rapport détaillé de feature engineering"""
        try:
            return {
                'timestamp': self.logger._get_hostname(),
                'feature_summary': {
                    'total_features_created': self.feature_metrics['features_created'],
                    'feature_engineering_score': self.feature_metrics['feature_engineering_score']
                },
                'feature_categories': {
                    'temporal_features': self.feature_metrics['temporal_features'],
                    'weather_features': self.feature_metrics['weather_features'],
                    'geographic_features': self.feature_metrics['geographic_features'],
                    'infrastructure_features': self.feature_metrics['infrastructure_features'],
                    'categorical_encodings': self.feature_metrics['categorical_encodings']
                },
                'feature_details': {
                    'temporal_features_list': [
                        'accident_hour', 'accident_day_of_week', 'accident_month',
                        'accident_season', 'is_weekend', 'is_rush_hour'
                    ],
                    'weather_features_list': [
                        'weather_category', 'weather_severity_score',
                        'visibility_category', 'temperature_category'
                    ],
                    'geographic_features_list': [
                        'distance_to_city_center', 'urban_rural_classification',
                        'state_region', 'population_density_category'
                    ],
                    'infrastructure_features_list': [
                        'infrastructure_count', 'safety_equipment_score',
                        'traffic_control_type'
                    ]
                }
            }
            
        except Exception as e:
            self.logger.error("Feature report generation failed", exception=e)
            return {'error': str(e)}