"""
Composant SchemaValidator pour la validation stricte du schéma US-Accidents
"""

from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DataType

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.utils.validation_utils import ValidationUtils
from ...common.exceptions.custom_exceptions import SchemaValidationError, DataValidationError


class SchemaValidator:
    """
    Validateur de schéma spécialisé pour le dataset US-Accidents
    
    Fonctionnalités:
    - Schéma US-Accidents pré-défini avec types exacts (47 colonnes)
    - Validation structure (nombre colonnes, noms, types)
    - Détection colonnes manquantes/supplémentaires
    - Rapports de validation détaillés
    - Validation des types de données stricte
    - Suggestions de correction automatique
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le validateur de schéma"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("schema_validator", self.config_manager)
        self.validation_utils = ValidationUtils(self.config_manager)
        
        # Schéma de référence US-Accidents (47 colonnes exactes)
        self.reference_schema = self.validation_utils.get_us_accidents_schema()
        self.expected_columns = self.config_manager.get('data.expected_columns', 47)
        
        # Colonnes critiques qui ne peuvent pas être manquantes
        self.critical_columns = [
            'ID', 'Severity', 'Start_Time', 'Start_Lat', 'Start_Lng', 'State'
        ]
        
        # Mapping des types de données acceptables
        self.acceptable_type_mappings = {
            'StringType': ['StringType'],
            'IntegerType': ['IntegerType', 'LongType', 'DoubleType'],  # Conversion possible
            'DoubleType': ['DoubleType', 'FloatType', 'IntegerType'],
            'TimestampType': ['TimestampType', 'StringType'],  # String peut être converti
            'BooleanType': ['BooleanType', 'StringType']  # String peut être converti
        }
        
        # Colonnes US-Accidents avec leurs types attendus
        self.us_accidents_columns = [
            'ID', 'Source', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng',
            'End_Lat', 'End_Lng', 'Distance(mi)', 'Description', 'Street', 'City', 'County',
            'State', 'Zipcode', 'Country', 'Timezone', 'Airport_Code', 'Weather_Timestamp',
            'Temperature(F)', 'Wind_Chill(F)', 'Humidity(%)', 'Pressure(in)', 'Visibility(mi)',
            'Wind_Direction', 'Wind_Speed(mph)', 'Precipitation(in)', 'Weather_Condition',
            'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
            'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
            'Turning_Loop', 'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
            'Astronomical_Twilight'
        ]
    
    def validate_dataframe_schema(self, df: DataFrame, strict_mode: bool = True) -> Dict[str, Any]:
        """
        Valide le schéma complet d'un DataFrame contre le schéma US-Accidents
        
        Args:
            df: DataFrame à valider
            strict_mode: Si True, applique une validation stricte
            
        Returns:
            Dict contenant les résultats de validation détaillés
            
        Raises:
            SchemaValidationError: Si la validation échoue en mode strict
        """
        try:
            self.logger.info("Starting schema validation", 
                           actual_columns=len(df.columns),
                           expected_columns=self.expected_columns,
                           strict_mode=strict_mode)
            
            # Validation de base avec ValidationUtils
            base_validation = self.validation_utils.validate_schema(df, self.reference_schema)
            
            # Validation étendue spécifique
            extended_validation = self._perform_extended_validation(df)
            
            # Combinaison des résultats
            validation_result = self._combine_validation_results(base_validation, extended_validation)
            
            # Génération du rapport détaillé
            detailed_report = self._generate_detailed_report(df, validation_result)
            
            # Logging des résultats
            self._log_validation_results(validation_result)
            
            # Vérification du mode strict
            if strict_mode and not validation_result['is_valid']:
                self._raise_schema_validation_error(validation_result)
            
            return detailed_report
            
        except Exception as e:
            self.logger.error("Schema validation failed", exception=e)
            if isinstance(e, SchemaValidationError):
                raise
            else:
                raise SchemaValidationError(
                    message=f"Schema validation error: {str(e)}",
                    expected_schema=str(self.reference_schema),
                    actual_schema=str(df.schema) if df else "unknown"
                )
    
    def _perform_extended_validation(self, df: DataFrame) -> Dict[str, Any]:
        """Effectue une validation étendue du schéma"""
        try:
            actual_columns = set(df.columns)
            expected_columns = set(self.us_accidents_columns)
            
            # Analyse des colonnes
            missing_columns = expected_columns - actual_columns
            extra_columns = actual_columns - expected_columns
            common_columns = actual_columns & expected_columns
            
            # Validation des colonnes critiques
            missing_critical = [col for col in self.critical_columns if col in missing_columns]
            
            # Analyse des types de données
            type_analysis = self._analyze_data_types(df)
            
            # Score de conformité
            conformity_score = self._calculate_conformity_score(
                len(missing_columns), len(extra_columns), len(missing_critical), type_analysis
            )
            
            return {
                'column_analysis': {
                    'missing_columns': list(missing_columns),
                    'extra_columns': list(extra_columns),
                    'common_columns': list(common_columns),
                    'missing_critical': missing_critical,
                    'column_count_match': len(actual_columns) == len(expected_columns)
                },
                'type_analysis': type_analysis,
                'conformity_score': conformity_score,
                'critical_issues': len(missing_critical) > 0
            }
            
        except Exception as e:
            self.logger.error("Extended validation failed", exception=e)
            return {'error': str(e)}
    
    def _analyze_data_types(self, df: DataFrame) -> Dict[str, Any]:
        """Analyse les types de données du DataFrame"""
        try:
            type_issues = []
            type_matches = []
            convertible_types = []
            
            expected_fields = {field.name: field for field in self.reference_schema.fields}
            
            for field in df.schema.fields:
                column_name = field.name
                actual_type = type(field.dataType).__name__
                
                if column_name in expected_fields:
                    expected_field = expected_fields[column_name]
                    expected_type = type(expected_field.dataType).__name__
                    
                    if actual_type == expected_type:
                        type_matches.append({
                            'column': column_name,
                            'type': actual_type
                        })
                    elif self._is_type_convertible(actual_type, expected_type):
                        convertible_types.append({
                            'column': column_name,
                            'actual_type': actual_type,
                            'expected_type': expected_type,
                            'convertible': True
                        })
                    else:
                        type_issues.append({
                            'column': column_name,
                            'actual_type': actual_type,
                            'expected_type': expected_type,
                            'convertible': False
                        })
            
            return {
                'type_matches': type_matches,
                'type_issues': type_issues,
                'convertible_types': convertible_types,
                'match_percentage': (len(type_matches) / len(df.columns)) * 100 if df.columns else 0
            }
            
        except Exception as e:
            self.logger.error("Data type analysis failed", exception=e)
            return {'error': str(e)}
    
    def _is_type_convertible(self, actual_type: str, expected_type: str) -> bool:
        """Vérifie si un type peut être converti vers le type attendu"""
        acceptable_types = self.acceptable_type_mappings.get(expected_type, [])
        return actual_type in acceptable_types
    
    def _calculate_conformity_score(self, missing_count: int, extra_count: int, 
                                  critical_missing: int, type_analysis: Dict[str, Any]) -> float:
        """Calcule un score de conformité du schéma"""
        try:
            base_score = 100.0
            
            # Pénalités pour colonnes manquantes
            base_score -= missing_count * 5
            
            # Pénalités pour colonnes supplémentaires
            base_score -= extra_count * 2
            
            # Pénalités critiques pour colonnes critiques manquantes
            base_score -= critical_missing * 20
            
            # Pénalités pour types incompatibles
            type_issues = type_analysis.get('type_issues', [])
            base_score -= len(type_issues) * 10
            
            # Bonus pour types convertibles
            convertible_types = type_analysis.get('convertible_types', [])
            base_score += len(convertible_types) * 2
            
            return max(0.0, min(100.0, base_score))
            
        except Exception:
            return 0.0
    
    def _combine_validation_results(self, base_result: Dict[str, Any],
                                  extended_result: Dict[str, Any]) -> Dict[str, Any]:
        """Combine les résultats de validation de base et étendue"""
        try:
            combined_result = base_result.copy()
            
            # Ensure is_valid key exists
            if 'is_valid' not in combined_result:
                combined_result['is_valid'] = True
            
            # Ajout des résultats étendus
            if 'error' not in extended_result:
                combined_result.update({
                    'extended_validation': extended_result,
                    'conformity_score': extended_result.get('conformity_score', 0.0),
                    'critical_issues': extended_result.get('critical_issues', False)
                })
                
                # Mise à jour du statut de validité
                if extended_result.get('critical_issues', False):
                    combined_result['is_valid'] = False
            
            return combined_result
            
        except Exception as e:
            self.logger.error("Failed to combine validation results", exception=e)
            return base_result
    
    def _generate_detailed_report(self, df: DataFrame, validation_result: Dict[str, Any]) -> Dict[str, Any]:
        """Génère un rapport détaillé de validation"""
        try:
            report = {
                'validation_timestamp': self.logger._get_hostname(),  # Utilise la méthode du logger
                'dataset_info': {
                    'total_columns': len(df.columns),
                    'total_rows': df.count() if validation_result.get('is_valid', False) else 'unknown',
                    'actual_columns': df.columns
                },
                'schema_validation': validation_result,
                'recommendations': self._generate_recommendations(validation_result),
                'severity_level': self._determine_severity_level(validation_result)
            }
            
            return report
            
        except Exception as e:
            self.logger.error("Failed to generate detailed report", exception=e)
            return validation_result
    
    def _generate_recommendations(self, validation_result: Dict[str, Any]) -> List[str]:
        """Génère des recommandations basées sur les résultats de validation"""
        recommendations = []
        
        try:
            extended_validation = validation_result.get('extended_validation', {})
            column_analysis = extended_validation.get('column_analysis', {})
            type_analysis = extended_validation.get('type_analysis', {})
            
            # Recommandations pour colonnes manquantes
            missing_columns = column_analysis.get('missing_columns', [])
            if missing_columns:
                recommendations.append(
                    f"Add missing columns: {', '.join(missing_columns[:5])}"
                    + (f" and {len(missing_columns) - 5} more" if len(missing_columns) > 5 else "")
                )
            
            # Recommandations pour colonnes supplémentaires
            extra_columns = column_analysis.get('extra_columns', [])
            if extra_columns:
                recommendations.append(
                    f"Remove extra columns: {', '.join(extra_columns[:3])}"
                    + (f" and {len(extra_columns) - 3} more" if len(extra_columns) > 3 else "")
                )
            
            # Recommandations pour types de données
            type_issues = type_analysis.get('type_issues', [])
            if type_issues:
                recommendations.append(
                    f"Fix data type mismatches for {len(type_issues)} columns"
                )
            
            # Recommandations pour types convertibles
            convertible_types = type_analysis.get('convertible_types', [])
            if convertible_types:
                recommendations.append(
                    f"Consider converting {len(convertible_types)} columns to expected types"
                )
            
            # Recommandations critiques
            missing_critical = column_analysis.get('missing_critical', [])
            if missing_critical:
                recommendations.insert(0, 
                    f"CRITICAL: Add missing critical columns: {', '.join(missing_critical)}"
                )
            
            if not recommendations:
                recommendations.append("Schema validation passed - no recommendations needed")
            
            return recommendations
            
        except Exception as e:
            self.logger.error("Failed to generate recommendations", exception=e)
            return ["Unable to generate recommendations due to error"]
    
    def _determine_severity_level(self, validation_result: Dict[str, Any]) -> str:
        """Détermine le niveau de sévérité des problèmes de schéma"""
        try:
            if validation_result.get('critical_issues', False):
                return "CRITICAL"
            
            conformity_score = validation_result.get('conformity_score', 100.0)
            
            if conformity_score >= 90:
                return "LOW"
            elif conformity_score >= 70:
                return "MEDIUM"
            elif conformity_score >= 50:
                return "HIGH"
            else:
                return "CRITICAL"
                
        except Exception:
            return "UNKNOWN"
    
    def _log_validation_results(self, validation_result: Dict[str, Any]):
        """Log les résultats de validation"""
        try:
            self.logger.log_data_quality("schema_validation_completed", {
                'is_valid': validation_result.get('is_valid', False),
                'conformity_score': validation_result.get('conformity_score', 0.0),
                'critical_issues': validation_result.get('critical_issues', False),
                'missing_columns_count': len(validation_result.get('missing_columns', [])),
                'extra_columns_count': len(validation_result.get('extra_columns', [])),
                'type_mismatches_count': len(validation_result.get('type_mismatches', []))
            })
            
        except Exception as e:
            self.logger.error("Failed to log validation results", exception=e)
    
    def _raise_schema_validation_error(self, validation_result: Dict[str, Any]):
        """Lève une exception de validation de schéma avec détails"""
        extended_validation = validation_result.get('extended_validation', {})
        column_analysis = extended_validation.get('column_analysis', {})
        
        raise SchemaValidationError(
            message="Schema validation failed in strict mode",
            expected_schema=str(self.reference_schema),
            actual_schema="See validation details",
            missing_columns=column_analysis.get('missing_columns', []),
            extra_columns=column_analysis.get('extra_columns', [])
        )
    
    def validate_column_names_only(self, df: DataFrame) -> Dict[str, Any]:
        """Valide uniquement les noms de colonnes (validation rapide)"""
        try:
            actual_columns = set(df.columns)
            expected_columns = set(self.us_accidents_columns)
            
            missing_columns = expected_columns - actual_columns
            extra_columns = actual_columns - expected_columns
            
            is_valid = len(missing_columns) == 0 and len(extra_columns) == 0
            
            result = {
                'is_valid': is_valid,
                'missing_columns': list(missing_columns),
                'extra_columns': list(extra_columns),
                'column_count_match': len(actual_columns) == len(expected_columns),
                'validation_type': 'column_names_only'
            }
            
            self.logger.info("Column names validation completed", **result)
            
            return result
            
        except Exception as e:
            self.logger.error("Column names validation failed", exception=e)
            return {'is_valid': False, 'error': str(e)}
    
    def get_reference_schema(self) -> StructType:
        """Retourne le schéma de référence US-Accidents"""
        return self.reference_schema
    
    def get_expected_columns(self) -> List[str]:
        """Retourne la liste des colonnes attendues"""
        return self.us_accidents_columns.copy()