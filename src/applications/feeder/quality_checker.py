"""
Composant QualityChecker pour le contrôle qualité des données US-Accidents
"""

from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, isnan, isnull, when, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, stddev, regexp_extract, length, trim, upper, lower,
    abs as spark_abs, round as spark_round
)
from pyspark.sql.types import DoubleType, IntegerType

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.utils.validation_utils import ValidationUtils
from ...common.exceptions.custom_exceptions import DataQualityError, DataValidationError


class QualityChecker:
    """
    Contrôleur de qualité spécialisé pour les données US-Accidents
    
    Fonctionnalités:
    - Validation coordonnées GPS (latitude/longitude valides)
    - Vérification codes d'état américains (State column)
    - Détection outliers (Distance, Temperature, etc.)
    - Calcul métriques qualité (% nulls, doublons, etc.)
    - Nettoyage données corrompues
    - Rapports de qualité détaillés
    - Scoring automatique de qualité
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le contrôleur de qualité"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("quality_checker", self.config_manager)
        self.validation_utils = ValidationUtils(self.config_manager)
        
        # Seuils de qualité configurables
        self.quality_thresholds = {
            'max_null_percentage': 30.0,
            'max_duplicate_percentage': 5.0,
            'min_quality_score': 70.0,
            'max_outlier_percentage': 10.0
        }
        
        # Règles de validation géographique (USA)
        self.geo_bounds = {
            'min_latitude': 24.0,    # Floride Keys
            'max_latitude': 71.0,    # Alaska
            'min_longitude': -180.0, # Alaska
            'max_longitude': -66.0   # Maine
        }
        
        # États américains valides (codes à 2 lettres)
        self.valid_us_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
        }
        
        # Plages de valeurs acceptables pour les métriques
        self.value_ranges = {
            'Severity': {'min': 1, 'max': 4},
            'Distance(mi)': {'min': 0.0, 'max': 100.0},
            'Temperature(F)': {'min': -50.0, 'max': 130.0},
            'Wind_Speed(mph)': {'min': 0.0, 'max': 200.0},
            'Humidity(%)': {'min': 0.0, 'max': 100.0},
            'Pressure(in)': {'min': 25.0, 'max': 35.0},
            'Visibility(mi)': {'min': 0.0, 'max': 50.0},
            'Precipitation(in)': {'min': 0.0, 'max': 20.0}
        }
        
        # Métriques de qualité
        self.quality_metrics = {}
    
    def check_data_quality(self, df: DataFrame, detailed: bool = True) -> Dict[str, Any]:
        """
        Effectue un contrôle qualité complet des données
        
        Args:
            df: DataFrame à analyser
            detailed: Si True, effectue une analyse détaillée
            
        Returns:
            Dict contenant les métriques de qualité et le score global
        """
        try:
            self.logger.info("Starting comprehensive data quality check", 
                           total_rows=df.count(),
                           total_columns=len(df.columns),
                           detailed=detailed)
            
            # Métriques de base
            base_metrics = self._calculate_base_metrics(df)
            
            # Validation géographique
            geo_validation = self._validate_geographic_data(df)
            
            # Validation des états
            state_validation = self._validate_state_codes(df)
            
            # Validation des valeurs numériques
            numeric_validation = self._validate_numeric_ranges(df)
            
            # Détection des outliers
            outlier_detection = self._detect_outliers(df) if detailed else {}
            
            # Validation temporelle
            temporal_validation = self._validate_temporal_consistency(df)
            
            # Détection des doublons
            duplicate_analysis = self._analyze_duplicates(df)
            
            # Calcul du score de qualité global
            quality_score = self._calculate_quality_score([
                base_metrics, geo_validation, state_validation, 
                numeric_validation, temporal_validation, duplicate_analysis
            ])
            
            # Compilation des résultats
            quality_report = {
                'timestamp': self.logger._get_hostname(),  # Timestamp via logger
                'overall_quality_score': quality_score,
                'total_rows': base_metrics['total_rows'],
                'total_columns': base_metrics['total_columns'],
                'base_metrics': base_metrics,
                'geographic_validation': geo_validation,
                'state_validation': state_validation,
                'numeric_validation': numeric_validation,
                'temporal_validation': temporal_validation,
                'duplicate_analysis': duplicate_analysis,
                'quality_issues': self._compile_quality_issues([
                    geo_validation, state_validation, numeric_validation, 
                    temporal_validation, duplicate_analysis
                ]),
                'recommendations': self._generate_quality_recommendations(quality_score)
            }
            
            if detailed:
                quality_report['outlier_detection'] = outlier_detection
            
            # Stockage des métriques
            self.quality_metrics = quality_report
            
            # Logging des résultats
            self._log_quality_results(quality_report)
            
            return quality_report
            
        except Exception as e:
            self.logger.error("Data quality check failed", exception=e)
            raise DataQualityError(
                message=f"Quality check failed: {str(e)}",
                quality_score=0.0,
                dataset="us_accidents"
            )
    
    def _calculate_base_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """Calcule les métriques de base du dataset"""
        try:
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Analyse des valeurs nulles par colonne
            null_analysis = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                null_analysis[column] = {
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2)
                }
            
            # Statistiques globales des nulls
            total_nulls = sum(metrics['null_count'] for metrics in null_analysis.values())
            avg_null_percentage = sum(metrics['null_percentage'] for metrics in null_analysis.values()) / total_columns
            
            return {
                'total_rows': total_rows,
                'total_columns': total_columns,
                'total_null_values': total_nulls,
                'average_null_percentage': round(avg_null_percentage, 2),
                'null_analysis_by_column': null_analysis,
                'high_null_columns': [
                    col for col, metrics in null_analysis.items() 
                    if metrics['null_percentage'] > self.quality_thresholds['max_null_percentage']
                ]
            }
            
        except Exception as e:
            self.logger.error("Base metrics calculation failed", exception=e)
            return {'error': str(e)}
    
    def _validate_geographic_data(self, df: DataFrame) -> Dict[str, Any]:
        """Valide les données géographiques (coordonnées GPS)"""
        try:
            if 'Start_Lat' not in df.columns or 'Start_Lng' not in df.columns:
                return {'error': 'Geographic columns not found'}
            
            total_rows = df.count()
            
            # Validation des coordonnées dans les limites USA
            invalid_coords = df.filter(
                (col('Start_Lat') < self.geo_bounds['min_latitude']) |
                (col('Start_Lat') > self.geo_bounds['max_latitude']) |
                (col('Start_Lng') < self.geo_bounds['min_longitude']) |
                (col('Start_Lng') > self.geo_bounds['max_longitude']) |
                col('Start_Lat').isNull() | col('Start_Lng').isNull()
            ).count()
            
            # Validation des coordonnées de fin si disponibles
            end_coord_validation = {}
            if 'End_Lat' in df.columns and 'End_Lng' in df.columns:
                invalid_end_coords = df.filter(
                    (col('End_Lat') < self.geo_bounds['min_latitude']) |
                    (col('End_Lat') > self.geo_bounds['max_latitude']) |
                    (col('End_Lng') < self.geo_bounds['min_longitude']) |
                    (col('End_Lng') > self.geo_bounds['max_longitude'])
                ).count()
                
                end_coord_validation = {
                    'invalid_end_coordinates': invalid_end_coords,
                    'end_coord_error_percentage': round((invalid_end_coords / total_rows) * 100, 2)
                }
            
            invalid_percentage = (invalid_coords / total_rows) * 100 if total_rows > 0 else 0
            
            # Analyse de la distribution géographique
            geo_distribution = self._analyze_geographic_distribution(df)
            
            return {
                'is_valid': invalid_coords == 0,
                'invalid_coordinates': invalid_coords,
                'invalid_percentage': round(invalid_percentage, 2),
                'total_validated': total_rows,
                'geographic_bounds': self.geo_bounds,
                'geographic_distribution': geo_distribution,
                **end_coord_validation
            }
            
        except Exception as e:
            self.logger.error("Geographic validation failed", exception=e)
            return {'error': str(e)}
    
    def _analyze_geographic_distribution(self, df: DataFrame) -> Dict[str, Any]:
        """Analyse la distribution géographique des accidents"""
        try:
            # Distribution par état (top 10)
            state_distribution = df.groupBy('State').count().orderBy(col('count').desc()).limit(10).collect()
            
            # Statistiques des coordonnées
            coord_stats = df.select(
                spark_min('Start_Lat').alias('min_lat'),
                spark_max('Start_Lat').alias('max_lat'),
                spark_min('Start_Lng').alias('min_lng'),
                spark_max('Start_Lng').alias('max_lng'),
                avg('Start_Lat').alias('avg_lat'),
                avg('Start_Lng').alias('avg_lng')
            ).collect()[0]
            
            return {
                'top_states': [{'state': row['State'], 'count': row['count']} for row in state_distribution],
                'coordinate_bounds': {
                    'min_latitude': coord_stats['min_lat'],
                    'max_latitude': coord_stats['max_lat'],
                    'min_longitude': coord_stats['min_lng'],
                    'max_longitude': coord_stats['max_lng'],
                    'center_latitude': round(coord_stats['avg_lat'], 4) if coord_stats['avg_lat'] else None,
                    'center_longitude': round(coord_stats['avg_lng'], 4) if coord_stats['avg_lng'] else None
                }
            }
            
        except Exception as e:
            self.logger.error("Geographic distribution analysis failed", exception=e)
            return {'error': str(e)}
    
    def _validate_state_codes(self, df: DataFrame) -> Dict[str, Any]:
        """Valide les codes d'état américains"""
        try:
            if 'State' not in df.columns:
                return {'error': 'State column not found'}
            
            total_rows = df.count()
            
            # Validation des codes d'état
            invalid_states = df.filter(
                ~col('State').isin(list(self.valid_us_states)) | col('State').isNull()
            ).count()
            
            # Analyse des états uniques
            unique_states = df.select('State').distinct().collect()
            found_states = {row['State'] for row in unique_states if row['State']}
            
            # États invalides trouvés
            invalid_state_codes = found_states - self.valid_us_states
            
            invalid_percentage = (invalid_states / total_rows) * 100 if total_rows > 0 else 0
            
            return {
                'is_valid': invalid_states == 0,
                'invalid_state_count': invalid_states,
                'invalid_percentage': round(invalid_percentage, 2),
                'total_unique_states': len(found_states),
                'valid_states_found': len(found_states & self.valid_us_states),
                'invalid_state_codes': list(invalid_state_codes),
                'coverage_percentage': round((len(found_states & self.valid_us_states) / len(self.valid_us_states)) * 100, 2)
            }
            
        except Exception as e:
            self.logger.error("State validation failed", exception=e)
            return {'error': str(e)}
    
    def _validate_numeric_ranges(self, df: DataFrame) -> Dict[str, Any]:
        """Valide les plages de valeurs numériques"""
        try:
            validation_results = {}
            
            for column, ranges in self.value_ranges.items():
                if column in df.columns:
                    total_rows = df.count()
                    
                    # Validation de la plage
                    out_of_range = df.filter(
                        (col(column) < ranges['min']) | 
                        (col(column) > ranges['max']) |
                        col(column).isNull()
                    ).count()
                    
                    # Statistiques descriptives
                    stats = df.select(
                        spark_min(column).alias('min_val'),
                        spark_max(column).alias('max_val'),
                        avg(column).alias('avg_val'),
                        stddev(column).alias('stddev_val')
                    ).collect()[0]
                    
                    validation_results[column] = {
                        'is_valid': out_of_range == 0,
                        'out_of_range_count': out_of_range,
                        'out_of_range_percentage': round((out_of_range / total_rows) * 100, 2) if total_rows > 0 else 0,
                        'expected_range': ranges,
                        'actual_range': {
                            'min': stats['min_val'],
                            'max': stats['max_val']
                        },
                        'statistics': {
                            'mean': round(stats['avg_val'], 2) if stats['avg_val'] else None,
                            'std_dev': round(stats['stddev_val'], 2) if stats['stddev_val'] else None
                        }
                    }
            
            # Score global de validation numérique
            valid_columns = sum(1 for result in validation_results.values() if result.get('is_valid', False))
            numeric_score = (valid_columns / len(validation_results)) * 100 if validation_results else 100
            
            return {
                'numeric_validation_score': round(numeric_score, 2),
                'columns_validated': len(validation_results),
                'columns_passed': valid_columns,
                'validation_details': validation_results
            }
            
        except Exception as e:
            self.logger.error("Numeric validation failed", exception=e)
            return {'error': str(e)}
    
    def _detect_outliers(self, df: DataFrame) -> Dict[str, Any]:
        """Détecte les outliers dans les colonnes numériques"""
        try:
            outlier_results = {}
            numeric_columns = ['Distance(mi)', 'Temperature(F)', 'Wind_Speed(mph)', 'Humidity(%)', 'Pressure(in)']
            
            for column in numeric_columns:
                if column in df.columns:
                    # Calcul des quartiles et IQR
                    quantiles = df.select(column).approxQuantile(column, [0.25, 0.75], 0.05)
                    if len(quantiles) == 2:
                        q1, q3 = quantiles
                        iqr = q3 - q1
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr
                        
                        # Détection des outliers
                        outliers = df.filter(
                            (col(column) < lower_bound) | (col(column) > upper_bound)
                        ).count()
                        
                        total_rows = df.count()
                        outlier_percentage = (outliers / total_rows) * 100 if total_rows > 0 else 0
                        
                        outlier_results[column] = {
                            'outlier_count': outliers,
                            'outlier_percentage': round(outlier_percentage, 2),
                            'bounds': {
                                'lower': round(lower_bound, 2),
                                'upper': round(upper_bound, 2)
                            },
                            'quartiles': {
                                'q1': round(q1, 2),
                                'q3': round(q3, 2),
                                'iqr': round(iqr, 2)
                            }
                        }
            
            return {
                'outlier_analysis': outlier_results,
                'columns_analyzed': len(outlier_results)
            }
            
        except Exception as e:
            self.logger.error("Outlier detection failed", exception=e)
            return {'error': str(e)}
    
    def _validate_temporal_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Valide la cohérence temporelle"""
        try:
            if 'Start_Time' not in df.columns:
                return {'error': 'Start_Time column not found'}
            
            total_rows = df.count()
            validation_results = {}
            
            # Validation Start_Time vs End_Time
            if 'End_Time' in df.columns:
                invalid_time_order = df.filter(col('End_Time') < col('Start_Time')).count()
                validation_results['time_order'] = {
                    'invalid_count': invalid_time_order,
                    'invalid_percentage': round((invalid_time_order / total_rows) * 100, 2) if total_rows > 0 else 0
                }
            
            # Validation des timestamps nulls
            null_start_time = df.filter(col('Start_Time').isNull()).count()
            validation_results['null_timestamps'] = {
                'null_start_time': null_start_time,
                'null_percentage': round((null_start_time / total_rows) * 100, 2) if total_rows > 0 else 0
            }
            
            # Analyse de la distribution temporelle
            temporal_distribution = self._analyze_temporal_distribution(df)
            
            return {
                'temporal_validation': validation_results,
                'temporal_distribution': temporal_distribution,
                'is_valid': all(
                    result.get('invalid_count', 0) == 0 
                    for result in validation_results.values()
                )
            }
            
        except Exception as e:
            self.logger.error("Temporal validation failed", exception=e)
            return {'error': str(e)}
    
    def _analyze_temporal_distribution(self, df: DataFrame) -> Dict[str, Any]:
        """Analyse la distribution temporelle des accidents"""
        try:
            from pyspark.sql.functions import year, month, dayofweek, hour
            
            # Distribution par année
            yearly_dist = df.select(year('Start_Time').alias('year')).groupBy('year').count().orderBy('year').collect()
            
            # Distribution par mois
            monthly_dist = df.select(month('Start_Time').alias('month')).groupBy('month').count().orderBy('month').collect()
            
            # Distribution par jour de la semaine
            weekly_dist = df.select(dayofweek('Start_Time').alias('day_of_week')).groupBy('day_of_week').count().orderBy('day_of_week').collect()
            
            return {
                'yearly_distribution': [{'year': row['year'], 'count': row['count']} for row in yearly_dist],
                'monthly_distribution': [{'month': row['month'], 'count': row['count']} for row in monthly_dist],
                'weekly_distribution': [{'day_of_week': row['day_of_week'], 'count': row['count']} for row in weekly_dist]
            }
            
        except Exception as e:
            self.logger.error("Temporal distribution analysis failed", exception=e)
            return {'error': str(e)}
    
    def _analyze_duplicates(self, df: DataFrame) -> Dict[str, Any]:
        """Analyse les doublons dans le dataset"""
        try:
            total_rows = df.count()
            
            # Doublons complets
            distinct_rows = df.distinct().count()
            duplicate_rows = total_rows - distinct_rows
            duplicate_percentage = (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0
            
            # Doublons basés sur l'ID
            id_duplicates = 0
            if 'ID' in df.columns:
                unique_ids = df.select('ID').distinct().count()
                id_duplicates = total_rows - unique_ids
            
            return {
                'total_rows': total_rows,
                'distinct_rows': distinct_rows,
                'duplicate_rows': duplicate_rows,
                'duplicate_percentage': round(duplicate_percentage, 2),
                'id_duplicates': id_duplicates,
                'is_acceptable': duplicate_percentage <= self.quality_thresholds['max_duplicate_percentage']
            }
            
        except Exception as e:
            self.logger.error("Duplicate analysis failed", exception=e)
            return {'error': str(e)}
    
    def _calculate_quality_score(self, validation_results: List[Dict[str, Any]]) -> float:
        """Calcule un score de qualité global"""
        try:
            scores = []
            
            for result in validation_results:
                if 'error' in result:
                    continue
                
                # Score basé sur différents critères
                if 'average_null_percentage' in result:  # Base metrics
                    null_score = max(0, 100 - result['average_null_percentage'])
                    scores.append(null_score)
                
                if 'invalid_percentage' in result:  # Geographic/State validation
                    invalid_score = max(0, 100 - result['invalid_percentage'])
                    scores.append(invalid_score)
                
                if 'numeric_validation_score' in result:  # Numeric validation
                    scores.append(result['numeric_validation_score'])
                
                if 'duplicate_percentage' in result:  # Duplicate analysis
                    dup_score = max(0, 100 - result['duplicate_percentage'])
                    scores.append(dup_score)
            
            return round(sum(scores) / len(scores), 2) if scores else 0.0
            
        except Exception as e:
            self.logger.error("Quality score calculation failed", exception=e)
            return 0.0
    
    def _compile_quality_issues(self, validation_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Compile les problèmes de qualité détectés"""
        issues = []
        
        try:
            for result in validation_results:
                if 'error' in result:
                    issues.append({
                        'type': 'validation_error',
                        'severity': 'high',
                        'message': result['error']
                    })
                
                # Issues géographiques
                if 'invalid_coordinates' in result and result['invalid_coordinates'] > 0:
                    issues.append({
                        'type': 'geographic_validation',
                        'severity': 'medium',
                        'message': f"{result['invalid_coordinates']} rows have invalid coordinates",
                        'count': result['invalid_coordinates']
                    })
                
                # Issues d'états
                if 'invalid_state_count' in result and result['invalid_state_count'] > 0:
                    issues.append({
                        'type': 'state_validation',
                        'severity': 'medium',
                        'message': f"{result['invalid_state_count']} rows have invalid state codes",
                        'count': result['invalid_state_count']
                    })
                
                # Issues de doublons
                if 'duplicate_percentage' in result and result['duplicate_percentage'] > self.quality_thresholds['max_duplicate_percentage']:
                    issues.append({
                        'type': 'duplicate_data',
                        'severity': 'low',
                        'message': f"{result['duplicate_percentage']:.2f}% duplicate rows detected",
                        'percentage': result['duplicate_percentage']
                    })
            
            return issues
            
        except Exception as e:
            self.logger.error("Failed to compile quality issues", exception=e)
            return [{'type': 'compilation_error', 'severity': 'high', 'message': str(e)}]
    
    def _generate_quality_recommendations(self, quality_score: float) -> List[str]:
        """Génère des recommandations basées sur le score de qualité"""
        recommendations = []
        
        try:
            if quality_score >= 90:
                recommendations.append("Excellent data quality - no immediate actions required")
            elif quality_score >= 80:
                recommendations.append("Good data quality - minor improvements possible")
                recommendations.append("Consider reviewing columns with high null percentages")
            elif quality_score >= 70:
                recommendations.append("Acceptable data quality - some improvements needed")
                recommendations.append("Focus on geographic and state code validation")
                recommendations.append("Review and clean outlier values")
            elif quality_score >= 50:
                recommendations.append("Poor data quality - significant improvements required")
                recommendations.append("Implement data cleaning pipeline")
                recommendations.append("Validate data sources and collection processes")
            else:
                recommendations.append("Critical data quality issues - immediate action required")
                recommendations.append("Consider data source replacement or major cleanup")
                recommendations.append("Implement comprehensive data validation")
            
            return recommendations
            
        except Exception as e:
            self.logger.error("Failed to generate recommendations", exception=e)
            return ["Unable to generate recommendations due to error"]
    
    def _log_quality_results(self, quality_report: Dict[str, Any]):
        """Log les résultats du contrôle qualité"""
        try:
            self.logger.log_data_quality("comprehensive_quality_check", {
                'overall_score': quality_report['overall_quality_score'],
                'total_rows': quality_report['total_rows'],
                'issues_count': len(quality_report['quality_issues']),
                'geographic_valid': quality_report['geographic_validation'].get('is_valid', False),
                'state_valid': quality_report['state_validation'].get('is_valid', False),
                'duplicate_percentage': quality_report['duplicate_analysis'].get('duplicate_percentage', 0)
            })
            
        except Exception as e:
            self.logger.error("Failed to log quality results", exception=e)
    
    def clean_corrupted_data(self, df: DataFrame, aggressive: bool = False) -> DataFrame:
        """
        Nettoie les données corrompues
        
        Args:
            df: DataFrame à nettoyer
            aggressive: Si True, applique un nettoyage plus agressif
            
        Returns:
            DataFrame nettoyé
        """
        try:
            self.logger.info("Starting data cleaning", 
                           original_rows=df.count(),
                           aggressive_mode=aggressive)
            
            cleaned_df = df
            
            # Nettoyage des coordonnées invalides
            if 'Start_Lat' in df.columns and 'Start_Lng' in df.columns:
                cleaned_df = cleaned_df.filter(
                    (col('Start_Lat') >= self.geo_bounds['min_latitude']) &
                    (col('Start_Lat') <= self.geo_bounds['max_latitude']) &
                    (col('Start_Lng') >= self.geo_bounds['min_longitude']) &
                    (col('Start_Lng') <= self.geo_bounds['max_longitude']) &
                    col('Start_Lat').isNotNull() & col('Start_Lng').isNotNull()
                )
            
            # Nettoyage des codes d'état invalides
            if 'State' in df.columns:
                cleaned_df = cleaned_df.filter(
                    col('State').isin(list(self.valid_us_states)) & col('State').isNotNull()
                )
            
            # Nettoyage des valeurs numériques hors plage
            for column, ranges in self.value_ranges.items():
                if column in cleaned_df.columns:
                    cleaned_df = cleaned_df.filter(
                        (col(column) >= ranges['min']) & 
                        (col(column) <= ranges['max']) |
                        col(column).isNull()
                    )
            
            # Suppression des doublons complets
            if aggressive:
                cleaned_df = cleaned_df.dropDuplicates()
            
            # Suppression des lignes avec trop de valeurs nulles
            if aggressive:
                # Supprime les lignes avec plus de 50% de valeurs nulles
                threshold = len(df.columns) * 0.5
                cleaned_df = cleaned_df.dropna(thresh=int(threshold))
            
            final_rows = cleaned_df.count()
            rows_removed = df.count() - final_rows
            removal_percentage = (rows_removed / df.count()) * 100 if df.count() > 0 else 0
            
            self.logger.log_performance(
                operation="data_cleaning",
                duration_seconds=0,
                original_rows=df.count(),
                final_rows=final_rows,
                rows_removed=rows_removed,
                removal_percentage=round(removal_percentage, 2)
            )
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error("Data cleaning failed", exception=e)
            return df
    
    def get_quality_metrics(self) -> Dict[str, Any]:
        """Retourne les dernières métriques de qualité calculées"""
        return self.quality_metrics.copy()
    
    def set_quality_thresholds(self, thresholds: Dict[str, float]):
        """Met à jour les seuils de qualité"""
        self.quality_thresholds.update(thresholds)
        self.logger.info("Quality thresholds updated", **thresholds)
    
    def validate_single_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valide un enregistrement individuel
        
        Args:
            record: Dictionnaire représentant un enregistrement
            
        Returns:
            Dict avec les résultats de validation
        """
        try:
            validation_results = {
                'is_valid': True,
                'issues': []
            }
            
            # Validation géographique
            if 'Start_Lat' in record and 'Start_Lng' in record:
                lat, lng = record['Start_Lat'], record['Start_Lng']
                if (lat is None or lng is None or
                    lat < self.geo_bounds['min_latitude'] or lat > self.geo_bounds['max_latitude'] or
                    lng < self.geo_bounds['min_longitude'] or lng > self.geo_bounds['max_longitude']):
                    validation_results['is_valid'] = False
                    validation_results['issues'].append('Invalid geographic coordinates')
            
            # Validation de l'état
            if 'State' in record:
                state = record['State']
                if state not in self.valid_us_states:
                    validation_results['is_valid'] = False
                    validation_results['issues'].append(f'Invalid state code: {state}')
            
            # Validation des plages numériques
            for column, ranges in self.value_ranges.items():
                if column in record and record[column] is not None:
                    value = record[column]
                    if value < ranges['min'] or value > ranges['max']:
                        validation_results['is_valid'] = False
                        validation_results['issues'].append(f'{column} out of range: {value}')
            
            return validation_results
            
        except Exception as e:
            return {
                'is_valid': False,
                'issues': [f'Validation error: {str(e)}']
            }