"""
Composant TransformationFactory pour la gestion centralisée des transformations
"""

from typing import Dict, Any, List, Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, coalesce, regexp_replace, trim, upper, lower,
    round as spark_round, abs as spark_abs, greatest, least,
    isnan, isnull, count, sum as spark_sum, avg, stddev,
    udf, broadcast, array, struct
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, BooleanType
from abc import ABC, abstractmethod

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import DataValidationError


class BaseTransformation(ABC):
    """Classe de base pour toutes les transformations"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.metrics = {'applied': False, 'records_affected': 0, 'execution_time': 0.0}
    
    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """Applique la transformation au DataFrame"""
        pass
    
    def validate_prerequisites(self, df: DataFrame) -> bool:
        """Valide les prérequis pour appliquer la transformation"""
        return True
    
    def get_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques de la transformation"""
        return self.metrics.copy()


class NullHandlingTransformation(BaseTransformation):
    """Transformation pour la gestion des valeurs nulles"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("null_handling", config)
        self.strategies = self.config.get('strategies', {})
        self.default_strategy = self.config.get('default_strategy', 'drop')
    
    def apply(self, df: DataFrame) -> DataFrame:
        import time
        start_time = time.time()
        
        try:
            transformed_df = df
            records_affected = 0
            
            for column in df.columns:
                if column in self.strategies:
                    strategy = self.strategies[column]
                    null_count = df.filter(col(column).isNull()).count()
                    
                    if null_count > 0:
                        if strategy == 'drop':
                            transformed_df = transformed_df.filter(col(column).isNotNull())
                        elif strategy == 'fill_zero':
                            transformed_df = transformed_df.fillna({column: 0})
                        elif strategy == 'fill_mean':
                            mean_val = df.select(avg(col(column))).collect()[0][0]
                            if mean_val is not None:
                                transformed_df = transformed_df.fillna({column: mean_val})
                        elif strategy == 'fill_unknown':
                            transformed_df = transformed_df.fillna({column: 'Unknown'})
                        
                        records_affected += null_count
            
            self.metrics.update({
                'applied': True,
                'records_affected': records_affected,
                'execution_time': time.time() - start_time
            })
            
            return transformed_df
            
        except Exception as e:
            self.metrics['applied'] = False
            raise DataValidationError(f"Null handling transformation failed: {str(e)}")


class OutlierRemovalTransformation(BaseTransformation):
    """Transformation pour la suppression des outliers"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("outlier_removal", config)
        self.z_threshold = self.config.get('z_threshold', 3.0)
        self.columns = self.config.get('columns', [])
    
    def apply(self, df: DataFrame) -> DataFrame:
        import time
        start_time = time.time()
        
        try:
            transformed_df = df
            initial_count = df.count()
            
            for column in self.columns:
                if column in df.columns:
                    # Calcul des statistiques
                    stats = df.select(
                        avg(col(column)).alias('mean'),
                        stddev(col(column)).alias('stddev')
                    ).collect()[0]
                    
                    if stats['mean'] is not None and stats['stddev'] is not None:
                        mean_val = stats['mean']
                        stddev_val = stats['stddev']
                        
                        # Suppression des outliers
                        transformed_df = transformed_df.filter(
                            spark_abs((col(column) - mean_val) / stddev_val) <= self.z_threshold
                        )
            
            final_count = transformed_df.count()
            records_affected = initial_count - final_count
            
            self.metrics.update({
                'applied': True,
                'records_affected': records_affected,
                'execution_time': time.time() - start_time
            })
            
            return transformed_df
            
        except Exception as e:
            self.metrics['applied'] = False
            raise DataValidationError(f"Outlier removal transformation failed: {str(e)}")


class DataTypeConversionTransformation(BaseTransformation):
    """Transformation pour la conversion des types de données"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("data_type_conversion", config)
        self.conversions = self.config.get('conversions', {})
    
    def apply(self, df: DataFrame) -> DataFrame:
        import time
        start_time = time.time()
        
        try:
            transformed_df = df
            records_affected = 0
            
            for column, target_type in self.conversions.items():
                if column in df.columns:
                    if target_type == 'double':
                        transformed_df = transformed_df.withColumn(column, col(column).cast(DoubleType()))
                    elif target_type == 'integer':
                        transformed_df = transformed_df.withColumn(column, col(column).cast(IntegerType()))
                    elif target_type == 'string':
                        transformed_df = transformed_df.withColumn(column, col(column).cast(StringType()))
                    elif target_type == 'boolean':
                        transformed_df = transformed_df.withColumn(column, col(column).cast(BooleanType()))
                    
                    records_affected += df.count()  # Toutes les lignes sont affectées
            
            self.metrics.update({
                'applied': True,
                'records_affected': records_affected,
                'execution_time': time.time() - start_time
            })
            
            return transformed_df
            
        except Exception as e:
            self.metrics['applied'] = False
            raise DataValidationError(f"Data type conversion transformation failed: {str(e)}")


class StringNormalizationTransformation(BaseTransformation):
    """Transformation pour la normalisation des chaînes de caractères"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("string_normalization", config)
        self.columns = self.config.get('columns', [])
        self.operations = self.config.get('operations', ['trim', 'upper'])
    
    def apply(self, df: DataFrame) -> DataFrame:
        import time
        start_time = time.time()
        
        try:
            transformed_df = df
            records_affected = 0
            
            for column in self.columns:
                if column in df.columns:
                    # Application des opérations de normalisation
                    if 'trim' in self.operations:
                        transformed_df = transformed_df.withColumn(column, trim(col(column)))
                    
                    if 'upper' in self.operations:
                        transformed_df = transformed_df.withColumn(column, upper(col(column)))
                    elif 'lower' in self.operations:
                        transformed_df = transformed_df.withColumn(column, lower(col(column)))
                    
                    if 'remove_extra_spaces' in self.operations:
                        transformed_df = transformed_df.withColumn(
                            column, 
                            regexp_replace(col(column), r'\s+', ' ')
                        )
                    
                    records_affected += df.count()
            
            self.metrics.update({
                'applied': True,
                'records_affected': records_affected,
                'execution_time': time.time() - start_time
            })
            
            return transformed_df
            
        except Exception as e:
            self.metrics['applied'] = False
            raise DataValidationError(f"String normalization transformation failed: {str(e)}")


class RangeValidationTransformation(BaseTransformation):
    """Transformation pour la validation des plages de valeurs"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("range_validation", config)
        self.ranges = self.config.get('ranges', {})
        self.action = self.config.get('action', 'filter')  # 'filter' ou 'clamp'
    
    def apply(self, df: DataFrame) -> DataFrame:
        import time
        start_time = time.time()
        
        try:
            transformed_df = df
            records_affected = 0
            
            for column, (min_val, max_val) in self.ranges.items():
                if column in df.columns:
                    if self.action == 'filter':
                        # Filtrage des valeurs hors plage
                        initial_count = transformed_df.count()
                        transformed_df = transformed_df.filter(
                            (col(column) >= min_val) & (col(column) <= max_val)
                        )
                        final_count = transformed_df.count()
                        records_affected += initial_count - final_count
                    
                    elif self.action == 'clamp':
                        # Écrêtage des valeurs hors plage
                        transformed_df = transformed_df.withColumn(
                            column,
                            greatest(least(col(column), lit(max_val)), lit(min_val))
                        )
                        records_affected += df.count()
            
            self.metrics.update({
                'applied': True,
                'records_affected': records_affected,
                'execution_time': time.time() - start_time
            })
            
            return transformed_df
            
        except Exception as e:
            self.metrics['applied'] = False
            raise DataValidationError(f"Range validation transformation failed: {str(e)}")


class TransformationFactory:
    """
    Factory pour la création et gestion des transformations
    
    Fonctionnalités:
    - Création de transformations configurables
    - Application séquentielle de transformations
    - Gestion des dépendances entre transformations
    - Métriques et logging détaillé
    - Rollback en cas d'échec
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise la factory de transformations"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("transformation_factory", self.config_manager)
        
        # Registry des transformations disponibles
        self.transformation_registry = {
            'null_handling': NullHandlingTransformation,
            'outlier_removal': OutlierRemovalTransformation,
            'data_type_conversion': DataTypeConversionTransformation,
            'string_normalization': StringNormalizationTransformation,
            'range_validation': RangeValidationTransformation
        }
        
        # Transformations appliquées
        self.applied_transformations = []
        
        # Métriques globales
        self.factory_metrics = {
            'transformations_created': 0,
            'transformations_applied': 0,
            'total_records_affected': 0,
            'total_execution_time': 0.0,
            'success_rate': 0.0
        }
    
    def create_transformation(self, transformation_type: str, config: Dict[str, Any] = None) -> BaseTransformation:
        """
        Crée une transformation du type spécifié
        
        Args:
            transformation_type: Type de transformation à créer
            config: Configuration spécifique à la transformation
            
        Returns:
            Instance de la transformation
        """
        try:
            if transformation_type not in self.transformation_registry:
                raise ValueError(f"Unknown transformation type: {transformation_type}")
            
            transformation_class = self.transformation_registry[transformation_type]
            transformation = transformation_class(config)
            
            self.factory_metrics['transformations_created'] += 1
            
            self.logger.info("Transformation created", 
                           transformation_type=transformation_type,
                           config=config)
            
            return transformation
            
        except Exception as e:
            self.logger.error("Failed to create transformation", 
                            exception=e, 
                            transformation_type=transformation_type)
            raise
    
    def apply_transformation_pipeline(self, df: DataFrame, 
                                    pipeline_config: List[Dict[str, Any]]) -> DataFrame:
        """
        Applique une pipeline de transformations
        
        Args:
            df: DataFrame d'entrée
            pipeline_config: Configuration de la pipeline
            
        Returns:
            DataFrame transformé
        """
        try:
            self.logger.info("Starting transformation pipeline", 
                           pipeline_steps=len(pipeline_config))
            
            transformed_df = df
            pipeline_start_time = time.time()
            
            for step_config in pipeline_config:
                transformation_type = step_config.get('type')
                transformation_config = step_config.get('config', {})
                
                if not transformation_type:
                    self.logger.warning("Skipping step without type", step_config=step_config)
                    continue
                
                # Création de la transformation
                transformation = self.create_transformation(transformation_type, transformation_config)
                
                # Validation des prérequis
                if not transformation.validate_prerequisites(transformed_df):
                    self.logger.warning("Prerequisites not met, skipping transformation", 
                                      transformation_type=transformation_type)
                    continue
                
                # Application de la transformation
                try:
                    transformed_df = transformation.apply(transformed_df)
                    self.applied_transformations.append(transformation)
                    self.factory_metrics['transformations_applied'] += 1
                    
                    # Mise à jour des métriques
                    metrics = transformation.get_metrics()
                    self.factory_metrics['total_records_affected'] += metrics.get('records_affected', 0)
                    self.factory_metrics['total_execution_time'] += metrics.get('execution_time', 0.0)
                    
                    self.logger.info("Transformation applied successfully", 
                                   transformation_type=transformation_type,
                                   records_affected=metrics.get('records_affected', 0))
                    
                except Exception as e:
                    self.logger.error("Transformation failed", 
                                    exception=e, 
                                    transformation_type=transformation_type)
                    
                    # Décision de continuer ou d'arrêter selon la configuration
                    if step_config.get('critical', False):
                        raise DataValidationError(f"Critical transformation failed: {transformation_type}")
                    else:
                        continue
            
            # Calcul du taux de succès
            if self.factory_metrics['transformations_created'] > 0:
                self.factory_metrics['success_rate'] = (
                    self.factory_metrics['transformations_applied'] / 
                    self.factory_metrics['transformations_created']
                ) * 100
            
            pipeline_duration = time.time() - pipeline_start_time
            
            self.logger.log_performance(
                operation="transformation_pipeline_complete",
                duration_seconds=pipeline_duration,
                transformations_applied=self.factory_metrics['transformations_applied'],
                total_records_affected=self.factory_metrics['total_records_affected'],
                success_rate=self.factory_metrics['success_rate']
            )
            
            return transformed_df
            
        except Exception as e:
            self.logger.error("Transformation pipeline failed", exception=e)
            raise
    
    def create_us_accidents_pipeline(self) -> List[Dict[str, Any]]:
        """Crée une pipeline de transformations spécifique aux données US-Accidents"""
        return [
            {
                'type': 'null_handling',
                'config': {
                    'strategies': {
                        'ID': 'drop',
                        'Start_Time': 'drop',
                        'Start_Lat': 'drop',
                        'Start_Lng': 'drop',
                        'State': 'drop',
                        'Severity': 'fill_zero',
                        'Temperature(F)': 'fill_mean',
                        'Humidity(%)': 'fill_mean',
                        'Weather_Condition': 'fill_unknown'
                    }
                },
                'critical': True
            },
            {
                'type': 'range_validation',
                'config': {
                    'ranges': {
                        'Start_Lat': (-90, 90),
                        'Start_Lng': (-180, 180),
                        'Severity': (1, 4),
                        'Temperature(F)': (-50, 150),
                        'Humidity(%)': (0, 100),
                        'Visibility(mi)': (0, 50),
                        'Wind_Speed(mph)': (0, 200)
                    },
                    'action': 'filter'
                },
                'critical': False
            },
            {
                'type': 'string_normalization',
                'config': {
                    'columns': ['State', 'City', 'Weather_Condition', 'Wind_Direction'],
                    'operations': ['trim', 'upper', 'remove_extra_spaces']
                },
                'critical': False
            },
            {
                'type': 'outlier_removal',
                'config': {
                    'columns': ['Distance(mi)', 'Temperature(F)', 'Wind_Speed(mph)', 'Visibility(mi)'],
                    'z_threshold': 3.0
                },
                'critical': False
            },
            {
                'type': 'data_type_conversion',
                'config': {
                    'conversions': {
                        'Severity': 'integer',
                        'Start_Lat': 'double',
                        'Start_Lng': 'double',
                        'Distance(mi)': 'double',
                        'Temperature(F)': 'double',
                        'Humidity(%)': 'double',
                        'Visibility(mi)': 'double',
                        'Wind_Speed(mph)': 'double'
                    }
                },
                'critical': False
            }
        ]
    
    def register_custom_transformation(self, name: str, transformation_class: type):
        """Enregistre une transformation personnalisée"""
        try:
            if not issubclass(transformation_class, BaseTransformation):
                raise ValueError("Transformation class must inherit from BaseTransformation")
            
            self.transformation_registry[name] = transformation_class
            
            self.logger.info("Custom transformation registered", 
                           transformation_name=name,
                           transformation_class=transformation_class.__name__)
            
        except Exception as e:
            self.logger.error("Failed to register custom transformation", 
                            exception=e, 
                            transformation_name=name)
            raise
    
    def get_factory_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques de la factory"""
        return self.factory_metrics.copy()
    
    def get_applied_transformations_summary(self) -> List[Dict[str, Any]]:
        """Retourne un résumé des transformations appliquées"""
        summary = []
        
        for transformation in self.applied_transformations:
            summary.append({
                'name': transformation.name,
                'metrics': transformation.get_metrics(),
                'config': transformation.config
            })
        
        return summary
    
    def reset_factory(self):
        """Remet à zéro la factory"""
        self.applied_transformations.clear()
        self.factory_metrics = {
            'transformations_created': 0,
            'transformations_applied': 0,
            'total_records_affected': 0,
            'total_execution_time': 0.0,
            'success_rate': 0.0
        }
        
        self.logger.info("Transformation factory reset")