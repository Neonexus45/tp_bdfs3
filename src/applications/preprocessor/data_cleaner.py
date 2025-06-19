"""
Composant DataCleaner pour le nettoyage et normalisation des données US-Accidents
"""

from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, trim, upper, lower,
    coalesce, lit, round as spark_round, abs as spark_abs,
    to_timestamp, date_format, split, regexp_extract,
    percentile_approx, stddev, mean, count, sum as spark_sum
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, BooleanType

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import DataValidationError


class DataCleaner:
    """
    Composant de nettoyage et normalisation des données US-Accidents
    
    Fonctionnalités:
    - Gestion des valeurs nulles, doublons et outliers pour les 47 colonnes
    - Normalisation des colonnes avec parenthèses (Distance(mi), Temperature(F), etc.)
    - Standardisation des formats (dates, coordonnées, codes)
    - Correction des données incohérentes
    - Validation et nettoyage des types de données
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le composant DataCleaner"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("data_cleaner", self.config_manager)
        
        # Configuration de nettoyage
        self.max_null_percentage = 30.0  # Seuil maximum de nulls par colonne
        self.outlier_threshold = 3.0  # Seuil Z-score pour les outliers
        
        # Colonnes avec parenthèses à normaliser
        self.columns_with_parentheses = {
            'Distance(mi)': 'distance_miles',
            'Temperature(F)': 'temperature_fahrenheit',
            'Wind_Chill(F)': 'wind_chill_fahrenheit',
            'Humidity(%)': 'humidity_percent',
            'Pressure(in)': 'pressure_inches',
            'Visibility(mi)': 'visibility_miles',
            'Wind_Speed(mph)': 'wind_speed_mph',
            'Precipitation(in)': 'precipitation_inches'
        }
        
        # Colonnes booléennes d'infrastructure
        self.infrastructure_columns = [
            'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
            'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming',
            'Traffic_Signal', 'Turning_Loop'
        ]
        
        # Colonnes temporelles
        self.temporal_columns = [
            'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight'
        ]
        
        # Métriques de nettoyage
        self.cleaning_metrics = {
            'records_input': 0,
            'records_output': 0,
            'duplicates_removed': 0,
            'nulls_handled': 0,
            'outliers_removed': 0,
            'columns_normalized': 0,
            'data_corrections': 0,
            'cleaning_score': 0.0
        }
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Nettoie et normalise les données US-Accidents
        
        Args:
            df: DataFrame avec les données brutes
            
        Returns:
            DataFrame nettoyé et normalisé
        """
        try:
            self.cleaning_metrics['records_input'] = df.count()
            
            self.logger.info("Starting data cleaning", 
                           input_records=self.cleaning_metrics['records_input'],
                           input_columns=len(df.columns))
            
            # Étape 1: Suppression des doublons
            cleaned_df = self._remove_duplicates(df)
            
            # Étape 2: Gestion des valeurs nulles
            cleaned_df = self._handle_null_values(cleaned_df)
            
            # Étape 3: Normalisation des colonnes avec parenthèses
            cleaned_df = self._normalize_column_names(cleaned_df)
            
            # Étape 4: Nettoyage des types de données
            cleaned_df = self._clean_data_types(cleaned_df)
            
            # Étape 5: Standardisation des formats
            cleaned_df = self._standardize_formats(cleaned_df)
            
            # Étape 6: Correction des données incohérentes
            cleaned_df = self._correct_inconsistent_data(cleaned_df)
            
            # Étape 7: Suppression des outliers
            cleaned_df = self._remove_outliers(cleaned_df)
            
            # Étape 8: Validation finale
            cleaned_df = self._final_validation(cleaned_df)
            
            # Calcul des métriques finales
            self._calculate_cleaning_metrics(cleaned_df)
            
            self.logger.log_performance(
                operation="data_cleaning_complete",
                duration_seconds=0,
                input_records=self.cleaning_metrics['records_input'],
                output_records=self.cleaning_metrics['records_output'],
                cleaning_score=self.cleaning_metrics['cleaning_score']
            )
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error("Data cleaning failed", exception=e)
            raise DataValidationError(
                message=f"Data cleaning failed: {str(e)}",
                validation_type="data_cleaning",
                dataset="us_accidents"
            )
    
    def _remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Supprime les doublons basés sur l'ID"""
        try:
            initial_count = df.count()
            
            # Suppression des doublons basée sur l'ID
            deduplicated_df = df.dropDuplicates(['ID'])
            
            final_count = deduplicated_df.count()
            duplicates_removed = initial_count - final_count
            
            self.cleaning_metrics['duplicates_removed'] = duplicates_removed
            
            if duplicates_removed > 0:
                self.logger.info("Duplicates removed", 
                               duplicates_count=duplicates_removed,
                               duplicate_percentage=round((duplicates_removed / initial_count) * 100, 2))
            
            return deduplicated_df
            
        except Exception as e:
            self.logger.error("Duplicate removal failed", exception=e)
            return df
    
    def _handle_null_values(self, df: DataFrame) -> DataFrame:
        """Gère les valeurs nulles selon des stratégies spécifiques"""
        try:
            cleaned_df = df
            nulls_handled = 0
            
            # Stratégies de gestion des nulls par type de colonne
            for column in df.columns:
                if column in cleaned_df.columns:
                    null_count = cleaned_df.filter(col(column).isNull()).count()
                    total_count = cleaned_df.count()
                    null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
                    
                    if null_percentage > 0:
                        cleaned_df = self._handle_column_nulls(cleaned_df, column, null_percentage)
                        nulls_handled += null_count
            
            self.cleaning_metrics['nulls_handled'] = nulls_handled
            
            self.logger.info("Null values handled", 
                           nulls_handled=nulls_handled)
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error("Null handling failed", exception=e)
            return df
    
    def _handle_column_nulls(self, df: DataFrame, column: str, null_percentage: float) -> DataFrame:
        """Gère les nulls pour une colonne spécifique"""
        try:
            # Colonnes critiques - pas de nulls acceptés
            critical_columns = ['ID', 'Start_Time', 'Start_Lat', 'Start_Lng', 'State']
            
            if column in critical_columns:
                # Suppression des lignes avec nulls dans les colonnes critiques
                return df.filter(col(column).isNotNull())
            
            # Colonnes numériques - remplacement par la médiane
            elif column in self.columns_with_parentheses or df.schema[column].dataType in [DoubleType(), IntegerType()]:
                if null_percentage < self.max_null_percentage:
                    median_value = df.select(percentile_approx(col(column), 0.5)).collect()[0][0]
                    if median_value is not None:
                        return df.fillna({column: median_value})
                return df.filter(col(column).isNotNull())
            
            # Colonnes booléennes - remplacement par False
            elif column in self.infrastructure_columns:
                return df.fillna({column: False})
            
            # Colonnes string - remplacement par 'Unknown'
            elif df.schema[column].dataType == StringType():
                if null_percentage < self.max_null_percentage:
                    return df.fillna({column: 'Unknown'})
                return df.filter(col(column).isNotNull())
            
            return df
            
        except Exception as e:
            self.logger.error("Column null handling failed", exception=e, column=column)
            return df
    
    def _normalize_column_names(self, df: DataFrame) -> DataFrame:
        """Normalise les noms de colonnes avec parenthèses"""
        try:
            normalized_df = df
            columns_normalized = 0
            
            for old_name, new_name in self.columns_with_parentheses.items():
                if old_name in normalized_df.columns:
                    normalized_df = normalized_df.withColumnRenamed(old_name, new_name)
                    columns_normalized += 1
                    
                    self.logger.debug("Column normalized", 
                                    old_name=old_name, 
                                    new_name=new_name)
            
            self.cleaning_metrics['columns_normalized'] = columns_normalized
            
            self.logger.info("Column names normalized", 
                           columns_normalized=columns_normalized)
            
            return normalized_df
            
        except Exception as e:
            self.logger.error("Column normalization failed", exception=e)
            return df
    
    def _clean_data_types(self, df: DataFrame) -> DataFrame:
        """Nettoie et valide les types de données"""
        try:
            cleaned_df = df
            
            # Nettoyage des coordonnées géographiques
            if 'Start_Lat' in cleaned_df.columns and 'Start_Lng' in cleaned_df.columns:
                cleaned_df = cleaned_df.filter(
                    (col('Start_Lat').between(-90, 90)) &
                    (col('Start_Lng').between(-180, 180))
                )
            
            # Nettoyage de la sévérité
            if 'Severity' in cleaned_df.columns:
                cleaned_df = cleaned_df.filter(
                    col('Severity').between(1, 4)
                )
            
            # Nettoyage des valeurs numériques négatives inappropriées
            numeric_positive_columns = ['distance_miles', 'visibility_miles', 'wind_speed_mph']
            for column in numeric_positive_columns:
                if column in cleaned_df.columns:
                    cleaned_df = cleaned_df.filter(col(column) >= 0)
            
            # Nettoyage des pourcentages
            if 'humidity_percent' in cleaned_df.columns:
                cleaned_df = cleaned_df.filter(
                    col('humidity_percent').between(0, 100)
                )
            
            self.logger.info("Data types cleaned")
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error("Data type cleaning failed", exception=e)
            return df
    
    def _standardize_formats(self, df: DataFrame) -> DataFrame:
        """Standardise les formats des données"""
        try:
            cleaned_df = df
            corrections = 0
            
            # Standardisation des codes d'état
            if 'State' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn('State', upper(trim(col('State'))))
                corrections += 1
            
            # Standardisation des noms de ville
            if 'City' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'City', 
                    regexp_replace(trim(col('City')), r'\s+', ' ')
                )
                corrections += 1
            
            # Standardisation des conditions météo
            if 'Weather_Condition' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'Weather_Condition',
                    trim(col('Weather_Condition'))
                )
                corrections += 1
            
            # Standardisation des directions du vent
            if 'Wind_Direction' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'Wind_Direction',
                    upper(trim(col('Wind_Direction')))
                )
                corrections += 1
            
            # Standardisation des valeurs temporelles
            for column in self.temporal_columns:
                if column in cleaned_df.columns:
                    cleaned_df = cleaned_df.withColumn(
                        column,
                        when(col(column).isin(['Day', 'day', 'DAY']), 'Day')
                        .when(col(column).isin(['Night', 'night', 'NIGHT']), 'Night')
                        .otherwise(col(column))
                    )
                    corrections += 1
            
            self.cleaning_metrics['data_corrections'] += corrections
            
            self.logger.info("Formats standardized", 
                           corrections_made=corrections)
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error("Format standardization failed", exception=e)
            return df
    
    def _correct_inconsistent_data(self, df: DataFrame) -> DataFrame:
        """Corrige les données incohérentes"""
        try:
            cleaned_df = df
            corrections = 0
            
            # Correction des températures extrêmes
            if 'temperature_fahrenheit' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'temperature_fahrenheit',
                    when(col('temperature_fahrenheit') < -50, lit(None))
                    .when(col('temperature_fahrenheit') > 150, lit(None))
                    .otherwise(col('temperature_fahrenheit'))
                )
                corrections += 1
            
            # Correction des vitesses de vent extrêmes
            if 'wind_speed_mph' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'wind_speed_mph',
                    when(col('wind_speed_mph') > 200, lit(None))
                    .otherwise(col('wind_speed_mph'))
                )
                corrections += 1
            
            # Correction des distances négatives
            if 'distance_miles' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'distance_miles',
                    when(col('distance_miles') < 0, spark_abs(col('distance_miles')))
                    .otherwise(col('distance_miles'))
                )
                corrections += 1
            
            # Correction des pressions atmosphériques
            if 'pressure_inches' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'pressure_inches',
                    when(col('pressure_inches') < 25, lit(None))
                    .when(col('pressure_inches') > 35, lit(None))
                    .otherwise(col('pressure_inches'))
                )
                corrections += 1
            
            self.cleaning_metrics['data_corrections'] += corrections
            
            self.logger.info("Inconsistent data corrected", 
                           corrections_made=corrections)
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error("Data correction failed", exception=e)
            return df
    
    def _remove_outliers(self, df: DataFrame) -> DataFrame:
        """Supprime les outliers statistiques"""
        try:
            cleaned_df = df
            initial_count = df.count()
            
            # Colonnes numériques pour la détection d'outliers
            numeric_columns = ['distance_miles', 'temperature_fahrenheit', 'wind_speed_mph', 'visibility_miles']
            
            for column in numeric_columns:
                if column in cleaned_df.columns:
                    # Calcul des statistiques
                    stats = cleaned_df.select(
                        mean(col(column)).alias('mean'),
                        stddev(col(column)).alias('stddev')
                    ).collect()[0]
                    
                    if stats['mean'] is not None and stats['stddev'] is not None:
                        mean_val = stats['mean']
                        stddev_val = stats['stddev']
                        
                        # Suppression des outliers (Z-score > threshold)
                        cleaned_df = cleaned_df.filter(
                            spark_abs((col(column) - mean_val) / stddev_val) <= self.outlier_threshold
                        )
            
            final_count = cleaned_df.count()
            outliers_removed = initial_count - final_count
            
            self.cleaning_metrics['outliers_removed'] = outliers_removed
            
            if outliers_removed > 0:
                self.logger.info("Outliers removed", 
                               outliers_count=outliers_removed,
                               outlier_percentage=round((outliers_removed / initial_count) * 100, 2))
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error("Outlier removal failed", exception=e)
            return df
    
    def _final_validation(self, df: DataFrame) -> DataFrame:
        """Validation finale des données nettoyées"""
        try:
            # Vérification des colonnes critiques
            critical_columns = ['ID', 'Start_Time', 'Start_Lat', 'Start_Lng', 'State']
            
            for column in critical_columns:
                if column in df.columns:
                    null_count = df.filter(col(column).isNull()).count()
                    if null_count > 0:
                        self.logger.warning("Critical column has nulls after cleaning", 
                                          column=column, 
                                          null_count=null_count)
            
            # Vérification des types de données
            expected_types = {
                'Severity': IntegerType(),
                'Start_Lat': DoubleType(),
                'Start_Lng': DoubleType(),
                'distance_miles': DoubleType()
            }
            
            for column, expected_type in expected_types.items():
                if column in df.columns:
                    actual_type = df.schema[column].dataType
                    if type(actual_type) != type(expected_type):
                        self.logger.warning("Type mismatch after cleaning", 
                                          column=column,
                                          expected=str(expected_type),
                                          actual=str(actual_type))
            
            self.logger.info("Final validation completed")
            
            return df
            
        except Exception as e:
            self.logger.error("Final validation failed", exception=e)
            return df
    
    def _calculate_cleaning_metrics(self, cleaned_df: DataFrame):
        """Calcule les métriques finales de nettoyage"""
        try:
            self.cleaning_metrics['records_output'] = cleaned_df.count()
            
            # Calcul du score de nettoyage
            input_records = self.cleaning_metrics['records_input']
            output_records = self.cleaning_metrics['records_output']
            
            if input_records > 0:
                retention_rate = (output_records / input_records) * 100
                
                # Score basé sur la rétention et les corrections
                base_score = min(retention_rate, 95)  # Max 95% pour la rétention
                
                # Bonus pour les corrections effectuées
                corrections_bonus = min(self.cleaning_metrics['data_corrections'] * 2, 10)
                
                # Malus pour trop de données perdues
                if retention_rate < 80:
                    retention_penalty = (80 - retention_rate) * 2
                    base_score -= retention_penalty
                
                cleaning_score = max(0, min(100, base_score + corrections_bonus))
                self.cleaning_metrics['cleaning_score'] = round(cleaning_score, 2)
            else:
                self.cleaning_metrics['cleaning_score'] = 0.0
            
        except Exception as e:
            self.logger.error("Cleaning metrics calculation failed", exception=e)
            self.cleaning_metrics['cleaning_score'] = 0.0
    
    def get_cleaning_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques de nettoyage"""
        return self.cleaning_metrics.copy()
    
    def get_cleaning_report(self) -> Dict[str, Any]:
        """Génère un rapport détaillé de nettoyage"""
        try:
            return {
                'timestamp': self.logger._get_hostname(),
                'cleaning_summary': {
                    'input_records': self.cleaning_metrics['records_input'],
                    'output_records': self.cleaning_metrics['records_output'],
                    'retention_rate': round((self.cleaning_metrics['records_output'] / 
                                           max(self.cleaning_metrics['records_input'], 1)) * 100, 2),
                    'cleaning_score': self.cleaning_metrics['cleaning_score']
                },
                'operations_performed': {
                    'duplicates_removed': self.cleaning_metrics['duplicates_removed'],
                    'nulls_handled': self.cleaning_metrics['nulls_handled'],
                    'outliers_removed': self.cleaning_metrics['outliers_removed'],
                    'columns_normalized': self.cleaning_metrics['columns_normalized'],
                    'data_corrections': self.cleaning_metrics['data_corrections']
                },
                'column_transformations': {
                    'normalized_columns': list(self.columns_with_parentheses.values()),
                    'infrastructure_columns': self.infrastructure_columns,
                    'temporal_columns': self.temporal_columns
                }
            }
            
        except Exception as e:
            self.logger.error("Cleaning report generation failed", exception=e)
            return {'error': str(e)}