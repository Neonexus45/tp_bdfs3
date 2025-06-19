"""
Module PREPROCESSOR pour la transformation et nettoyage des données US-Accidents

Ce module contient tous les composants nécessaires pour transformer les données
de la couche Bronze vers la couche Silver avec feature engineering avancé.
"""

from .preprocessor_app import PreprocessorApp
from .data_cleaner import DataCleaner
from .feature_engineer import FeatureEngineer
from .hive_table_manager import HiveTableManager
from .transformation_factory import TransformationFactory, BaseTransformation
from .aggregation_engine import AggregationEngine

__all__ = [
    'PreprocessorApp',
    'DataCleaner', 
    'FeatureEngineer',
    'HiveTableManager',
    'TransformationFactory',
    'BaseTransformation',
    'AggregationEngine'
]

__version__ = "1.0.0"
__author__ = "Lakehouse Team"
__description__ = "Advanced data preprocessing and feature engineering for US-Accidents dataset"