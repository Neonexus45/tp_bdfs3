from typing import Dict, Any, List, Optional, Union, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql.functions import col, isnan, isnull, when, count, sum as spark_sum, avg, min as spark_min, max as spark_max
import re
from datetime import datetime
from ..config.config_manager import ConfigManager
from .logger import Logger


class ValidationUtils:
    """Utilitaires de validation des schémas et données"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("validation_utils", self.config_manager)
        self.expected_columns = self.config_manager.get('data.expected_columns', 47)
    
    def get_us_accidents_schema(self) -> StructType:
        """Retourne le schéma attendu pour le dataset US Accidents"""
        return StructType([
            StructField("ID", StringType(), False),
            StructField("Severity", IntegerType(), True),
            StructField("Start_Time", TimestampType(), True),
            StructField("End_Time", TimestampType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("End_Lat", DoubleType(), True),
            StructField("End_Lng", DoubleType(), True),
            StructField("Distance(mi)", DoubleType(), True),
            StructField("Description", StringType(), True),
            StructField("Number", DoubleType(), True),
            StructField("Street", StringType(), True),
            StructField("Side", StringType(), True),
            StructField("City", StringType(), True),
            StructField("County", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Zipcode", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Timezone", StringType(), True),
            StructField("Airport_Code", StringType(), True),
            StructField("Weather_Timestamp", TimestampType(), True),
            StructField("Temperature(F)", DoubleType(), True),
            StructField("Wind_Chill(F)", DoubleType(), True),
            StructField("Humidity(%)", DoubleType(), True),
            StructField("Pressure(in)", DoubleType(), True),
            StructField("Visibility(mi)", DoubleType(), True),
            StructField("Wind_Direction", StringType(), True),
            StructField("Wind_Speed(mph)", DoubleType(), True),
            StructField("Precipitation(in)", DoubleType(), True),
            StructField("Weather_Condition", StringType(), True),
            StructField("Amenity", BooleanType(), True),
            StructField("Bump", BooleanType(), True),
            StructField("Crossing", BooleanType(), True),
            StructField("Give_Way", BooleanType(), True),
            StructField("Junction", BooleanType(), True),
            StructField("No_Exit", BooleanType(), True),
            StructField("Railway", BooleanType(), True),
            StructField("Roundabout", BooleanType(), True),
            StructField("Station", BooleanType(), True),
            StructField("Stop", BooleanType(), True),
            StructField("Traffic_Calming", BooleanType(), True),
            StructField("Traffic_Signal", BooleanType(), True),
            StructField("Turning_Loop", BooleanType(), True),
            StructField("Sunrise_Sunset", StringType(), True),
            StructField("Civil_Twilight", StringType(), True),
            StructField("Nautical_Twilight", StringType(), True),
            StructField("Astronomical_Twilight", StringType(), True),
        ])
    
    def validate_schema(self, df: DataFrame, expected_schema: StructType = None) -> Dict[str, Any]:
        """Valide le schéma d'un DataFrame"""
        try:
            if expected_schema is None:
                expected_schema = self.get_us_accidents_schema()
            
            actual_schema = df.schema
            validation_result = {
                'is_valid': True,
                'column_count_match': len(actual_schema.fields) == len(expected_schema.fields),
                'expected_columns': len(expected_schema.fields),
                'actual_columns': len(actual_schema.fields),
                'missing_columns': [],
                'extra_columns': [],
                'type_mismatches': [],
                'nullable_mismatches': []
            }
            
            expected_fields = {field.name: field for field in expected_schema.fields}
            actual_fields = {field.name: field for field in actual_schema.fields}
            
            for field_name, expected_field in expected_fields.items():
                if field_name not in actual_fields:
                    validation_result['missing_columns'].append(field_name)
                    validation_result['is_valid'] = False
                else:
                    actual_field = actual_fields[field_name]
                    if actual_field.dataType != expected_field.dataType:
                        validation_result['type_mismatches'].append({
                            'column': field_name,
                            'expected_type': str(expected_field.dataType),
                            'actual_type': str(actual_field.dataType)
                        })
                        validation_result['is_valid'] = False
                    
                    if actual_field.nullable != expected_field.nullable:
                        validation_result['nullable_mismatches'].append({
                            'column': field_name,
                            'expected_nullable': expected_field.nullable,
                            'actual_nullable': actual_field.nullable
                        })
            
            for field_name in actual_fields:
                if field_name not in expected_fields:
                    validation_result['extra_columns'].append(field_name)
            
            self.logger.log_data_quality("schema_validation", validation_result)
            
            return validation_result
            
        except Exception as e:
            self.logger.error("Schema validation failed", exception=e)
            return {'is_valid': False, 'error': str(e)}
    
    def check_data_quality(self, df: DataFrame, rules: Dict[str, Any] = None) -> Dict[str, Any]:
        """Vérifie la qualité des données selon des règles définies"""
        try:
            if rules is None:
                rules = self.get_default_quality_rules()
            
            total_rows = df.count()
            quality_metrics = {
                'total_rows': total_rows,
                'quality_score': 100.0,
                'issues': [],
                'column_metrics': {}
            }
            
            for column_name in df.columns:
                column_metrics = self._analyze_column_quality(df, column_name, rules.get(column_name, {}))
                quality_metrics['column_metrics'][column_name] = column_metrics
                
                if column_metrics['null_percentage'] > rules.get('max_null_percentage', 50):
                    quality_metrics['issues'].append({
                        'type': 'high_null_rate',
                        'column': column_name,
                        'null_percentage': column_metrics['null_percentage']
                    })
                    quality_metrics['quality_score'] -= 5
            
            if 'Severity' in df.columns:
                severity_check = self._validate_severity_values(df)
                if not severity_check['is_valid']:
                    quality_metrics['issues'].append(severity_check)
                    quality_metrics['quality_score'] -= 10
            
            if 'Start_Lat' in df.columns and 'Start_Lng' in df.columns:
                geo_check = self._validate_geographic_coordinates(df)
                if not geo_check['is_valid']:
                    quality_metrics['issues'].append(geo_check)
                    quality_metrics['quality_score'] -= 10
            
            if 'Start_Time' in df.columns and 'End_Time' in df.columns:
                time_check = self._validate_time_consistency(df)
                if not time_check['is_valid']:
                    quality_metrics['issues'].append(time_check)
                    quality_metrics['quality_score'] -= 10
            
            quality_metrics['quality_score'] = max(0, quality_metrics['quality_score'])
            
            self.logger.log_data_quality("data_quality_check", quality_metrics)
            
            return quality_metrics
            
        except Exception as e:
            self.logger.error("Data quality check failed", exception=e)
            return {'quality_score': 0, 'error': str(e)}
    
    def _analyze_column_quality(self, df: DataFrame, column_name: str, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Analyse la qualité d'une colonne spécifique"""
        try:
            total_rows = df.count()
            
            null_count = df.filter(col(column_name).isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            
            metrics = {
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2),
                'non_null_count': total_rows - null_count
            }
            
            if df.schema[column_name].dataType in [IntegerType(), DoubleType()]:
                numeric_stats = df.select(
                    spark_min(col(column_name)).alias('min_val'),
                    spark_max(col(column_name)).alias('max_val'),
                    avg(col(column_name)).alias('avg_val')
                ).collect()[0]
                
                metrics.update({
                    'min_value': numeric_stats['min_val'],
                    'max_value': numeric_stats['max_val'],
                    'avg_value': round(numeric_stats['avg_val'], 2) if numeric_stats['avg_val'] else None
                })
            
            elif df.schema[column_name].dataType == StringType():
                string_stats = df.select(
                    count(when(col(column_name) == "", True)).alias('empty_strings'),
                    count(when(col(column_name).rlike(r'^\s*$'), True)).alias('whitespace_only')
                ).collect()[0]
                
                metrics.update({
                    'empty_strings': string_stats['empty_strings'],
                    'whitespace_only': string_stats['whitespace_only']
                })
            
            return metrics
            
        except Exception as e:
            self.logger.error("Column quality analysis failed", exception=e, column=column_name)
            return {'error': str(e)}
    
    def _validate_severity_values(self, df: DataFrame) -> Dict[str, Any]:
        """Valide les valeurs de sévérité (1-4)"""
        try:
            invalid_severity = df.filter(
                (col('Severity') < 1) | (col('Severity') > 4) | col('Severity').isNull()
            ).count()
            
            total_rows = df.count()
            invalid_percentage = (invalid_severity / total_rows) * 100 if total_rows > 0 else 0
            
            return {
                'type': 'severity_validation',
                'is_valid': invalid_severity == 0,
                'invalid_count': invalid_severity,
                'invalid_percentage': round(invalid_percentage, 2),
                'message': f"{invalid_severity} rows have invalid severity values"
            }
            
        except Exception as e:
            return {'type': 'severity_validation', 'is_valid': False, 'error': str(e)}
    
    def _validate_geographic_coordinates(self, df: DataFrame) -> Dict[str, Any]:
        """Valide les coordonnées géographiques"""
        try:
            invalid_coords = df.filter(
                (col('Start_Lat') < -90) | (col('Start_Lat') > 90) |
                (col('Start_Lng') < -180) | (col('Start_Lng') > 180) |
                col('Start_Lat').isNull() | col('Start_Lng').isNull()
            ).count()
            
            total_rows = df.count()
            invalid_percentage = (invalid_coords / total_rows) * 100 if total_rows > 0 else 0
            
            return {
                'type': 'geographic_validation',
                'is_valid': invalid_coords == 0,
                'invalid_count': invalid_coords,
                'invalid_percentage': round(invalid_percentage, 2),
                'message': f"{invalid_coords} rows have invalid geographic coordinates"
            }
            
        except Exception as e:
            return {'type': 'geographic_validation', 'is_valid': False, 'error': str(e)}
    
    def _validate_time_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Valide la cohérence temporelle"""
        try:
            invalid_times = df.filter(
                col('End_Time') < col('Start_Time')
            ).count()
            
            total_rows = df.count()
            invalid_percentage = (invalid_times / total_rows) * 100 if total_rows > 0 else 0
            
            return {
                'type': 'time_consistency_validation',
                'is_valid': invalid_times == 0,
                'invalid_count': invalid_times,
                'invalid_percentage': round(invalid_percentage, 2),
                'message': f"{invalid_times} rows have end time before start time"
            }
            
        except Exception as e:
            return {'type': 'time_consistency_validation', 'is_valid': False, 'error': str(e)}
    
    def get_default_quality_rules(self) -> Dict[str, Any]:
        """Retourne les règles de qualité par défaut"""
        return {
            'max_null_percentage': 30,
            'ID': {'max_null_percentage': 0},
            'Severity': {'max_null_percentage': 5, 'valid_range': [1, 4]},
            'Start_Time': {'max_null_percentage': 5},
            'Start_Lat': {'max_null_percentage': 5, 'valid_range': [-90, 90]},
            'Start_Lng': {'max_null_percentage': 5, 'valid_range': [-180, 180]},
            'State': {'max_null_percentage': 5},
            'City': {'max_null_percentage': 10},
        }
    
    def validate_us_states(self, df: DataFrame, state_column: str = 'State') -> Dict[str, Any]:
        """Valide les codes d'état américains"""
        try:
            valid_states = {
                'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
            }
            
            invalid_states = df.filter(
                ~col(state_column).isin(list(valid_states)) | col(state_column).isNull()
            ).count()
            
            total_rows = df.count()
            invalid_percentage = (invalid_states / total_rows) * 100 if total_rows > 0 else 0
            
            return {
                'type': 'state_validation',
                'is_valid': invalid_states == 0,
                'invalid_count': invalid_states,
                'invalid_percentage': round(invalid_percentage, 2),
                'valid_states_count': len(valid_states),
                'message': f"{invalid_states} rows have invalid state codes"
            }
            
        except Exception as e:
            return {'type': 'state_validation', 'is_valid': False, 'error': str(e)}
    
    def generate_quality_report(self, df: DataFrame) -> Dict[str, Any]:
        """Génère un rapport complet de qualité des données"""
        try:
            schema_validation = self.validate_schema(df)
            data_quality = self.check_data_quality(df)
            state_validation = self.validate_us_states(df)
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'dataset_info': {
                    'total_rows': df.count(),
                    'total_columns': len(df.columns),
                    'columns': df.columns
                },
                'schema_validation': schema_validation,
                'data_quality': data_quality,
                'state_validation': state_validation,
                'overall_score': self._calculate_overall_score([
                    schema_validation.get('is_valid', False),
                    data_quality.get('quality_score', 0) > 70,
                    state_validation.get('is_valid', False)
                ])
            }
            
            self.logger.log_data_quality("quality_report_generated", {
                'overall_score': report['overall_score'],
                'total_rows': report['dataset_info']['total_rows']
            })
            
            return report
            
        except Exception as e:
            self.logger.error("Quality report generation failed", exception=e)
            return {'error': str(e)}
    
    def _calculate_overall_score(self, validations: List[bool]) -> float:
        """Calcule un score global de qualité"""
        if not validations:
            return 0.0
        
        return (sum(validations) / len(validations)) * 100