"""
Exceptions personnalisées pour le projet lakehouse
"""

from .custom_exceptions import (
    LakehouseException,
    ConfigurationError,
    DataValidationError,
    SparkJobError,
    DatabaseConnectionError,
    DataQualityError,
    SchemaValidationError
)

__all__ = [
    "LakehouseException",
    "ConfigurationError", 
    "DataValidationError",
    "SparkJobError",
    "DatabaseConnectionError",
    "DataQualityError",
    "SchemaValidationError"
]