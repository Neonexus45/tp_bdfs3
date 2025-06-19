"""
Application FEEDER pour l'ingestion quotidienne des données US-Accidents

Cette application implémente un pipeline complet d'ingestion de données avec:
- Lecture CSV avec schéma strict des 47 colonnes exactes
- Validation qualité des données (nulls, types, coordonnées GPS)
- Gestion erreurs et données corrompues avec logging détaillé
- Partitioning intelligent par date d'ingestion et état
- Compression et optimisation stockage Parquet (Snappy)
- Logging détaillé des métriques d'ingestion (JSON structuré)
- Optimisations Spark (coalesce, partition pruning, cache)

Composants principaux:
- FeederApp: Application principale avec orchestration du pipeline
- DataIngestion: Lecture optimisée des données CSV
- SchemaValidator: Validation stricte du schéma US-Accidents
- QualityChecker: Contrôle qualité et nettoyage des données
- PartitioningStrategy: Stratégie de partitioning intelligent

Usage:
    from src.applications.feeder import FeederApp
    
    feeder = FeederApp()
    result = feeder.run()
"""

from .feeder_app import FeederApp
from .data_ingestion import DataIngestion
from .schema_validator import SchemaValidator
from .quality_checker import QualityChecker
from .partitioning_strategy import PartitioningStrategy

__all__ = [
    'FeederApp',
    'DataIngestion',
    'SchemaValidator',
    'QualityChecker',
    'PartitioningStrategy'
]

# Version de l'application FEEDER
__version__ = '1.0.0'

# Métadonnées de l'application
__author__ = 'Lakehouse Accidents US Team'
__description__ = 'Application d\'ingestion quotidienne des données US-Accidents'
__license__ = 'MIT'