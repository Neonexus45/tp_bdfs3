"""
AnalyticsEngine - Moteur d'analyse avancée pour DATAMART
"""

import time
from typing import Dict, Any, Optional, List, Tuple
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
    concat_ws, countDistinct, corr
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import SparkJobError, DataValidationError


class AnalyticsEngine:
    """
    Moteur d'analyse avancée pour la couche Gold
    
    Fonctionnalités:
    - Calculs de corrélations et tendances
    - Détection d'anomalies dans les données
    - Recommandations automatiques pour infrastructure
    - Analyse de patterns complexes
    - Métriques de qualité des données
    - Insights business automatisés
    """
    
    def __init__(self, spark: SparkSession, config_manager: ConfigManager, logger: Logger):
        self.spark = spark
        self.config_manager = config_manager
        self.logger = logger
        
        # Configuration
        self.hive_database = config_manager.get('hive.database')
        
        # Seuils pour les analyses
        self.analysis_thresholds = {
            'correlation_significance': 0.3,    # Seuil de corrélation significative
            'anomaly_threshold': 2.0,           # Seuil d'anomalie (écarts-types)
            'trend_significance': 0.1,          # Seuil de significativité des tendances
            'min_data_points': 30               # Minimum de points pour analyse
        }
        
        self.logger.info("AnalyticsEngine initialized", 
                        hive_database=self.hive_database,
                        thresholds=self.analysis_thresholds)
    
    def calculate_data_quality_metrics(self) -> Dict[str, Any]:
        """
        Calcule les métriques de qualité des données Gold
        
        Returns:
            Dict avec métriques de qualité
        """
        try:
            self.logger.info("Starting data quality metrics calculation")
            start_time = time.time()
            
            quality_metrics = {}
            
            # Tables à analyser
            tables_to_analyze = [
                'accidents_clean',
                'weather_aggregated',
                'infrastructure_features'
            ]
            
            for table_name in tables_to_analyze:
                try:
                    df = self.spark.table(f"{self.hive_database}.{table_name}")
                    table_metrics = self._calculate_table_quality_metrics(df, table_name)
                    quality_metrics[table_name] = table_metrics
                    
                except Exception as e:
                    quality_metrics[table_name] = {
                        'error': str(e),
                        'status': 'FAILED'
                    }
                    self.logger.error(f"Failed to analyze quality for {table_name}", exception=e)
            
            # Métriques globales
            global_metrics = self._calculate_global_quality_metrics(quality_metrics)
            quality_metrics['global'] = global_metrics
            
            duration = time.time() - start_time
            
            self.logger.log_performance("data_quality_metrics_calculation", duration,
                                      tables_analyzed=len(tables_to_analyze))
            
            return quality_metrics
            
        except Exception as e:
            self.logger.error("Data quality metrics calculation failed", exception=e)
            return {'error': str(e), 'status': 'FAILED'}
    
    def detect_anomalies(self, aggregated_data: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        Détecte les anomalies dans les données agrégées
        
        Args:
            aggregated_data: Données agrégées par BusinessAggregator
            
        Returns:
            Dict avec anomalies détectées
        """
        try:
            self.logger.info("Starting anomaly detection")
            start_time = time.time()
            
            anomalies = {}
            
            # Détection d'anomalies temporelles
            if 'temporal' in aggregated_data:
                temporal_anomalies = self._detect_temporal_anomalies(aggregated_data['temporal'])
                anomalies['temporal'] = temporal_anomalies
            
            # Détection d'anomalies géographiques
            if 'geographic' in aggregated_data:
                geographic_anomalies = self._detect_geographic_anomalies(aggregated_data['geographic'])
                anomalies['geographic'] = geographic_anomalies
            
            # Détection d'anomalies météorologiques
            if 'weather' in aggregated_data:
                weather_anomalies = self._detect_weather_anomalies(aggregated_data['weather'])
                anomalies['weather'] = weather_anomalies
            
            # Résumé des anomalies
            anomaly_summary = self._summarize_anomalies(anomalies)
            anomalies['summary'] = anomaly_summary
            
            duration = time.time() - start_time
            
            self.logger.log_performance("anomaly_detection", duration,
                                      anomaly_types=len(anomalies))
            
            return anomalies
            
        except Exception as e:
            self.logger.error("Anomaly detection failed", exception=e)
            raise SparkJobError(f"Anomaly detection failed: {str(e)}") from e
    
    def calculate_correlations(self, aggregated_data: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        Calcule les corrélations entre différentes dimensions
        
        Args:
            aggregated_data: Données agrégées par BusinessAggregator
            
        Returns:
            Dict avec matrices de corrélation
        """
        try:
            self.logger.info("Starting correlation analysis")
            start_time = time.time()
            
            correlations = {}
            
            # Corrélations météo-accidents
            if 'weather' in aggregated_data:
                weather_correlations = self._calculate_weather_correlations(aggregated_data['weather'])
                correlations['weather_accidents'] = weather_correlations
            
            # Corrélations infrastructure-sécurité
            if 'infrastructure' in aggregated_data:
                infra_correlations = self._calculate_infrastructure_correlations(aggregated_data['infrastructure'])
                correlations['infrastructure_safety'] = infra_correlations
            
            # Corrélations temporelles
            if 'temporal' in aggregated_data:
                temporal_correlations = self._calculate_temporal_correlations(aggregated_data['temporal'])
                correlations['temporal_patterns'] = temporal_correlations
            
            # Corrélations croisées
            if 'cross_dimensional' in aggregated_data:
                cross_correlations = self._calculate_cross_correlations(aggregated_data['cross_dimensional'])
                correlations['cross_dimensional'] = cross_correlations
            
            duration = time.time() - start_time
            
            self.logger.log_performance("correlation_analysis", duration,
                                      correlation_types=len(correlations))
            
            return correlations
            
        except Exception as e:
            self.logger.error("Correlation analysis failed", exception=e)
            raise SparkJobError(f"Correlation analysis failed: {str(e)}") from e
    
    def generate_insights(self, kpis_data: Dict[str, DataFrame], 
                         correlations: Dict[str, Any], 
                         anomalies: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Génère des insights business automatisés
        
        Args:
            kpis_data: Données KPIs calculées
            correlations: Corrélations calculées
            anomalies: Anomalies détectées
            
        Returns:
            Liste d'insights business
        """
        try:
            self.logger.info("Starting insights generation")
            start_time = time.time()
            
            insights = []
            
            # Insights de sécurité
            security_insights = self._generate_security_insights(kpis_data.get('security'))
            insights.extend(security_insights)
            
            # Insights temporels
            temporal_insights = self._generate_temporal_insights(kpis_data.get('temporal'))
            insights.extend(temporal_insights)
            
            # Insights d'infrastructure
            infrastructure_insights = self._generate_infrastructure_insights(kpis_data.get('infrastructure'))
            insights.extend(infrastructure_insights)
            
            # Insights basés sur les corrélations
            correlation_insights = self._generate_correlation_insights(correlations)
            insights.extend(correlation_insights)
            
            # Insights basés sur les anomalies
            anomaly_insights = self._generate_anomaly_insights(anomalies)
            insights.extend(anomaly_insights)
            
            # Tri par priorité et pertinence
            insights.sort(key=lambda x: (x.get('priority', 5), -x.get('confidence', 0)))
            
            duration = time.time() - start_time
            
            self.logger.log_performance("insights_generation", duration,
                                      insights_generated=len(insights))
            
            return insights
            
        except Exception as e:
            self.logger.error("Insights generation failed", exception=e)
            return []
    
    def _calculate_table_quality_metrics(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """Calcule les métriques de qualité pour une table"""
        
        try:
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Calcul des valeurs nulles par colonne
            null_counts = {}
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = {
                    'null_count': null_count,
                    'null_percentage': round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
                }
            
            # Calcul de la complétude globale
            total_nulls = sum(nc['null_count'] for nc in null_counts.values())
            total_cells = total_rows * total_columns
            completeness = round((1 - (total_nulls / total_cells)) * 100, 2) if total_cells > 0 else 0
            
            # Détection de doublons (sur colonnes clés si disponibles)
            duplicate_count = 0
            if 'id' in df.columns:
                duplicate_count = df.count() - df.dropDuplicates(['id']).count()
            
            # Métriques de cohérence (exemples)
            consistency_issues = []
            
            # Vérification des coordonnées GPS si disponibles
            if 'start_lat' in df.columns and 'start_lng' in df.columns:
                invalid_coords = df.filter(
                    (col('start_lat') < -90) | (col('start_lat') > 90) |
                    (col('start_lng') < -180) | (col('start_lng') > 180)
                ).count()
                if invalid_coords > 0:
                    consistency_issues.append(f"Invalid GPS coordinates: {invalid_coords} rows")
            
            # Vérification des valeurs de sévérité si disponibles
            if 'severity' in df.columns:
                invalid_severity = df.filter(
                    (col('severity') < 1) | (col('severity') > 4)
                ).count()
                if invalid_severity > 0:
                    consistency_issues.append(f"Invalid severity values: {invalid_severity} rows")
            
            return {
                'total_rows': total_rows,
                'total_columns': total_columns,
                'completeness_percentage': completeness,
                'null_analysis': null_counts,
                'duplicate_count': duplicate_count,
                'consistency_issues': consistency_issues,
                'quality_score': self._calculate_quality_score(completeness, duplicate_count, len(consistency_issues), total_rows)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to calculate quality metrics for {table_name}", exception=e)
            return {'error': str(e)}
    
    def _calculate_quality_score(self, completeness: float, duplicates: int, 
                               consistency_issues: int, total_rows: int) -> float:
        """Calcule un score de qualité global"""
        
        # Score de base basé sur la complétude
        base_score = completeness
        
        # Pénalité pour les doublons
        duplicate_penalty = (duplicates / total_rows) * 20 if total_rows > 0 else 0
        
        # Pénalité pour les problèmes de cohérence
        consistency_penalty = consistency_issues * 5
        
        # Score final (entre 0 et 100)
        final_score = max(0, base_score - duplicate_penalty - consistency_penalty)
        
        return round(final_score, 2)
    
    def _calculate_global_quality_metrics(self, table_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calcule les métriques de qualité globales"""
        
        valid_tables = [metrics for metrics in table_metrics.values() 
                       if 'error' not in metrics]
        
        if not valid_tables:
            return {'error': 'No valid table metrics available'}
        
        # Score de qualité moyen
        avg_quality_score = sum(metrics['quality_score'] for metrics in valid_tables) / len(valid_tables)
        
        # Complétude moyenne
        avg_completeness = sum(metrics['completeness_percentage'] for metrics in valid_tables) / len(valid_tables)
        
        # Total des problèmes de cohérence
        total_consistency_issues = sum(len(metrics['consistency_issues']) for metrics in valid_tables)
        
        # Recommandations globales
        recommendations = []
        if avg_quality_score < 80:
            recommendations.append("Overall data quality is below acceptable threshold (80%)")
        if avg_completeness < 90:
            recommendations.append("Data completeness needs improvement")
        if total_consistency_issues > 0:
            recommendations.append(f"Address {total_consistency_issues} consistency issues across tables")
        
        return {
            'average_quality_score': round(avg_quality_score, 2),
            'average_completeness': round(avg_completeness, 2),
            'total_consistency_issues': total_consistency_issues,
            'tables_analyzed': len(valid_tables),
            'recommendations': recommendations
        }
    
    def _detect_temporal_anomalies(self, temporal_df: DataFrame) -> List[Dict[str, Any]]:
        """Détecte les anomalies temporelles"""
        
        anomalies = []
        
        try:
            # Calcul des statistiques pour détection d'anomalies
            stats = temporal_df.select(
                avg('accidents_count').alias('mean_accidents'),
                stddev('accidents_count').alias('stddev_accidents'),
                avg('avg_severity').alias('mean_severity'),
                stddev('avg_severity').alias('stddev_severity')
            ).collect()[0]
            
            mean_accidents = stats['mean_accidents']
            stddev_accidents = stats['stddev_accidents']
            mean_severity = stats['mean_severity']
            stddev_severity = stats['stddev_severity']
            
            # Détection d'anomalies dans le nombre d'accidents
            if stddev_accidents and stddev_accidents > 0:
                accident_anomalies = temporal_df.filter(
                    spark_abs(col('accidents_count') - mean_accidents) > 
                    (self.analysis_thresholds['anomaly_threshold'] * stddev_accidents)
                ).collect()
                
                for anomaly in accident_anomalies:
                    anomalies.append({
                        'type': 'temporal_accident_count',
                        'period_type': anomaly['period_type'],
                        'period_value': anomaly['period_value'],
                        'value': anomaly['accidents_count'],
                        'expected_range': f"{mean_accidents - 2*stddev_accidents:.0f} - {mean_accidents + 2*stddev_accidents:.0f}",
                        'severity': 'HIGH' if abs(anomaly['accidents_count'] - mean_accidents) > 3*stddev_accidents else 'MEDIUM'
                    })
            
            # Détection d'anomalies dans la sévérité
            if stddev_severity and stddev_severity > 0:
                severity_anomalies = temporal_df.filter(
                    spark_abs(col('avg_severity') - mean_severity) > 
                    (self.analysis_thresholds['anomaly_threshold'] * stddev_severity)
                ).collect()
                
                for anomaly in severity_anomalies:
                    anomalies.append({
                        'type': 'temporal_severity',
                        'period_type': anomaly['period_type'],
                        'period_value': anomaly['period_value'],
                        'value': anomaly['avg_severity'],
                        'expected_range': f"{mean_severity - 2*stddev_severity:.2f} - {mean_severity + 2*stddev_severity:.2f}",
                        'severity': 'HIGH' if abs(anomaly['avg_severity'] - mean_severity) > 3*stddev_severity else 'MEDIUM'
                    })
            
        except Exception as e:
            self.logger.error("Temporal anomaly detection failed", exception=e)
        
        return anomalies
    
    def _detect_geographic_anomalies(self, geographic_df: DataFrame) -> List[Dict[str, Any]]:
        """Détecte les anomalies géographiques"""
        
        anomalies = []
        
        try:
            # Détection des états avec des taux d'accidents anormalement élevés
            state_stats = geographic_df.filter(col('geographic_level') == 'state').select(
                avg('accidents_count').alias('mean_accidents'),
                stddev('accidents_count').alias('stddev_accidents')
            ).collect()[0]
            
            if state_stats['stddev_accidents'] and state_stats['stddev_accidents'] > 0:
                outlier_states = geographic_df.filter(
                    (col('geographic_level') == 'state') &
                    (col('accidents_count') > state_stats['mean_accidents'] + 
                     2 * state_stats['stddev_accidents'])
                ).collect()
                
                for state in outlier_states:
                    anomalies.append({
                        'type': 'geographic_hotspot',
                        'location': state['state'],
                        'value': state['accidents_count'],
                        'expected_max': state_stats['mean_accidents'] + 2 * state_stats['stddev_accidents'],
                        'severity': 'HIGH'
                    })
            
        except Exception as e:
            self.logger.error("Geographic anomaly detection failed", exception=e)
        
        return anomalies
    
    def _detect_weather_anomalies(self, weather_df: DataFrame) -> List[Dict[str, Any]]:
        """Détecte les anomalies météorologiques"""
        
        anomalies = []
        
        try:
            # Détection des conditions météo avec impact anormalement élevé
            weather_stats = weather_df.select(
                avg('severity_impact_score').alias('mean_impact'),
                stddev('severity_impact_score').alias('stddev_impact')
            ).collect()[0]
            
            if weather_stats['stddev_impact'] and weather_stats['stddev_impact'] > 0:
                high_impact_weather = weather_df.filter(
                    col('severity_impact_score') > weather_stats['mean_impact'] + 
                    2 * weather_stats['stddev_impact']
                ).collect()
                
                for weather in high_impact_weather:
                    anomalies.append({
                        'type': 'weather_high_impact',
                        'weather_factor': weather['weather_factor'],
                        'factor_type': weather['factor_type'],
                        'impact_score': weather['severity_impact_score'],
                        'expected_max': weather_stats['mean_impact'] + 2 * weather_stats['stddev_impact'],
                        'severity': 'MEDIUM'
                    })
            
        except Exception as e:
            self.logger.error("Weather anomaly detection failed", exception=e)
        
        return anomalies
    
    def _summarize_anomalies(self, anomalies: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Résume les anomalies détectées"""
        
        total_anomalies = sum(len(anomaly_list) for anomaly_list in anomalies.values())
        high_severity = sum(1 for anomaly_list in anomalies.values() 
                           for anomaly in anomaly_list 
                           if anomaly.get('severity') == 'HIGH')
        
        return {
            'total_anomalies': total_anomalies,
            'high_severity_anomalies': high_severity,
            'anomaly_types': list(anomalies.keys()),
            'requires_attention': high_severity > 0
        }
    
    def _calculate_weather_correlations(self, weather_df: DataFrame) -> Dict[str, Any]:
        """Calcule les corrélations météo-accidents"""
        
        try:
            # Préparation des données pour corrélation
            correlation_data = weather_df.select(
                col('accidents_count').cast('double'),
                col('avg_severity').cast('double'),
                col('avg_temperature').cast('double'),
                col('avg_humidity').cast('double'),
                col('avg_visibility').cast('double')
            ).filter(
                col('avg_temperature').isNotNull() &
                col('avg_humidity').isNotNull() &
                col('avg_visibility').isNotNull()
            )
            
            if correlation_data.count() < self.analysis_thresholds['min_data_points']:
                return {'error': 'Insufficient data for correlation analysis'}
            
            # Calcul des corrélations
            correlations = {}
            
            # Corrélation température-accidents
            temp_corr = correlation_data.select(
                corr('accidents_count', 'avg_temperature').alias('correlation')
            ).collect()[0]['correlation']
            
            correlations['temperature_accidents'] = {
                'correlation': round(temp_corr, 3) if temp_corr else 0,
                'significance': 'HIGH' if abs(temp_corr or 0) > self.analysis_thresholds['correlation_significance'] else 'LOW'
            }
            
            # Corrélation visibilité-sévérité
            vis_corr = correlation_data.select(
                corr('avg_severity', 'avg_visibility').alias('correlation')
            ).collect()[0]['correlation']
            
            correlations['visibility_severity'] = {
                'correlation': round(vis_corr, 3) if vis_corr else 0,
                'significance': 'HIGH' if abs(vis_corr or 0) > self.analysis_thresholds['correlation_significance'] else 'LOW'
            }
            
            return correlations
            
        except Exception as e:
            self.logger.error("Weather correlation calculation failed", exception=e)
            return {'error': str(e)}
    
    def _calculate_infrastructure_correlations(self, infrastructure_df: DataFrame) -> Dict[str, Any]:
        """Calcule les corrélations infrastructure-sécurité"""
        
        try:
            correlations = {}
            
            # Corrélation efficacité-réduction d'accidents
            effectiveness_corr = infrastructure_df.select(
                corr('equipment_effectiveness', 'accident_reduction_rate').alias('correlation')
            ).collect()[0]['correlation']
            
            correlations['effectiveness_reduction'] = {
                'correlation': round(effectiveness_corr, 3) if effectiveness_corr else 0,
                'significance': 'HIGH' if abs(effectiveness_corr or 0) > self.analysis_thresholds['correlation_significance'] else 'LOW'
            }
            
            return correlations
            
        except Exception as e:
            self.logger.error("Infrastructure correlation calculation failed", exception=e)
            return {'error': str(e)}
    
    def _calculate_temporal_correlations(self, temporal_df: DataFrame) -> Dict[str, Any]:
        """Calcule les corrélations temporelles"""
        
        try:
            correlations = {}
            
            # Corrélation facteur saisonnier-accidents
            seasonal_corr = temporal_df.select(
                corr('seasonal_factor', 'accidents_count').alias('correlation')
            ).collect()[0]['correlation']
            
            correlations['seasonal_accidents'] = {
                'correlation': round(seasonal_corr, 3) if seasonal_corr else 0,
                'significance': 'HIGH' if abs(seasonal_corr or 0) > self.analysis_thresholds['correlation_significance'] else 'LOW'
            }
            
            return correlations
            
        except Exception as e:
            self.logger.error("Temporal correlation calculation failed", exception=e)
            return {'error': str(e)}
    
    def _calculate_cross_correlations(self, cross_df: DataFrame) -> Dict[str, Any]:
        """Calcule les corrélations croisées"""
        
        try:
            correlations = {}
            
            # Corrélation score de risque combiné
            risk_corr = cross_df.select(
                corr('combined_risk_score', 'accidents_count').alias('correlation')
            ).collect()[0]['correlation']
            
            correlations['combined_risk'] = {
                'correlation': round(risk_corr, 3) if risk_corr else 0,
                'significance': 'HIGH' if abs(risk_corr or 0) > self.analysis_thresholds['correlation_significance'] else 'LOW'
            }
            
            return correlations
            
        except Exception as e:
            self.logger.error("Cross correlation calculation failed", exception=e)
            return {'error': str(e)}
    
    def _generate_security_insights(self, security_df: Optional[DataFrame]) -> List[Dict[str, Any]]:
        """Génère des insights de sécurité"""
        
        insights = []
        
        if security_df is None:
            return insights
        
        try:
            # Top 5 des états les plus dangereux
            top_dangerous = security_df.filter(
                col('city').isNull()  # États seulement
            ).orderBy(desc('danger_index')).limit(5).collect()
            
            if top_dangerous:
                insights.append({
                    'type': 'security',
                    'category': 'top_dangerous_states',
                    'title': 'États les plus dangereux identifiés',
                    'description': f"Les 5 états avec les indices de dangerosité les plus élevés: {', '.join([row['state'] for row in top_dangerous])}",
                    'priority': 1,
                    'confidence': 0.9,
                    'actionable': True,
                    'recommendation': 'Concentrer les efforts de sécurité routière sur ces états prioritaires'
                })
            
        except Exception as e:
            self.logger.error("Security insights generation failed", exception=e)
        
        return insights
    
    def _generate_temporal_insights(self, temporal_df: Optional[DataFrame]) -> List[Dict[str, Any]]:
        """Génère des insights temporels"""
        
        insights = []
        
        if temporal_df is None:
            return insights
        
        try:
            # Identification des pics horaires
            hourly_peaks = temporal_df.filter(
                col('period_type') == 'hourly'
            ).orderBy(desc('accidents_count')).limit(3).collect()
            
            if hourly_peaks:
                peak_hours = [row['period_value'] for row in hourly_peaks]
                insights.append({
                    'type': 'temporal',
                    'category': 'peak_hours',
                    'title': 'Heures de pointe identifiées',
                    'description': f"Les heures avec le plus d'accidents: {', '.join(peak_hours)}h",
                    'priority': 2,
                    'confidence': 0.8,
                    'actionable': True,
                    'recommendation': 'Renforcer la surveillance et les mesures préventives pendant ces heures'
                })
            
        except Exception as e:
            self.logger.error("Temporal insights generation failed", exception=e)
        
        return insights
    
    def _generate_infrastructure_insights(self, infrastructure_df: Optional[DataFrame]) -> List[Dict[str, Any]]:
        """Génère des insights d'infrastructure"""
        
        insights = []
        
        if infrastructure_df is None:
            return insights
        
        try:
            # Infrastructure la plus efficace
            most_effective = infrastructure_df.orderBy(desc('equipment_effectiveness')).limit(1).collect()
            
            if most_effective:
                best_infra = most_effective[0]
                insights.append({
                    'type': 'infrastructure',
                    'category': 'most_effective',
                    'title': 'Infrastructure la plus efficace identifiée',
                    'description': f"Type d'infrastructure le plus efficace: {best_infra['infrastructure_type']} (efficacité: {best_infra['equipment_effectiveness']:.2f})",
                    'priority': 2,
                    'confidence': 0.7,
                    'actionable': True,
                    'recommendation': f'Privilégier l\'installation de {best_infra["infrastructure_type"]} dans les zones à risque'
                })
            
        except Exception as e:
            self.logger.error("Infrastructure insights generation failed", exception=e)
        
        return insights
    
    def _generate_correlation_insights(self, correlations: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Génère des insights basés sur les corrélations"""
        
        insights = []
        
        try:
            # Analyse des corrélations significatives
            for corr_type, corr_data in correlations.items():
                if isinstance(corr_data, dict):
                    for corr_name, corr_info in corr_data.items():
                        if isinstance(corr_info, dict) and corr_info.get('significance') == 'HIGH':
                            correlation_value = corr_info.get('correlation', 0)
                            insights.append({
                                'type': 'correlation',
                                'category': corr_type,
                                'title': f'Corrélation significative détectée: {corr_name}',
                                'description': f'Corrélation de {correlation_value:.3f} entre les variables',
                                'priority': 3,
                                'confidence': 0.8,
                                'actionable': True,
                                'recommendation': f'Exploiter cette corrélation pour optimiser les stratégies de prévention'
                            })
        
        except Exception as e:
            self.logger.error("Correlation insights generation failed", exception=e)
        
        return insights
    
    def _generate_anomaly_insights(self, anomalies: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Génère des insights basés sur les anomalies"""
        
        insights = []
        
        try:
            anomaly_summary = anomalies.get('summary', {})
            
            if anomaly_summary.get('requires_attention', False):
                high_severity_count = anomaly_summary.get('high_severity_anomalies', 0)
                total_anomalies = anomaly_summary.get('total_anomalies', 0)
                
                insights.append({
                    'type': 'anomaly',
                    'category': 'high_priority',
                    'title': 'Anomalies critiques détectées',
                    'description': f'{high_severity_count} anomalies de haute sévérité sur {total_anomalies} au total',
                    'priority': 1,
                    'confidence': 0.9,
                    'actionable': True,
                    'recommendation': 'Investigation immédiate requise pour les anomalies de haute sévérité'
                })
        
        except Exception as e:
            self.logger.error("Anomaly insights generation failed", exception=e)
        
        return insights