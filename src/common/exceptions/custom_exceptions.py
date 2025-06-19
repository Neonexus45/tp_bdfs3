from typing import Dict, Any, Optional


class LakehouseException(Exception):
    """Exception de base pour le projet lakehouse"""
    
    def __init__(self, message: str, error_code: str = None, context: Dict[str, Any] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit l'exception en dictionnaire pour le logging"""
        return {
            'exception_type': self.__class__.__name__,
            'message': self.message,
            'error_code': self.error_code,
            'context': self.context
        }


class ConfigurationError(LakehouseException):
    """Erreur de configuration"""
    
    def __init__(self, message: str, config_key: str = None, config_value: Any = None):
        super().__init__(
            message=message,
            error_code="CONFIG_ERROR",
            context={
                'config_key': config_key,
                'config_value': config_value
            }
        )
        self.config_key = config_key
        self.config_value = config_value


class DataValidationError(LakehouseException):
    """Erreur de validation des données"""
    
    def __init__(self, message: str, validation_type: str = None, 
                 failed_rules: list = None, dataset: str = None):
        super().__init__(
            message=message,
            error_code="DATA_VALIDATION_ERROR",
            context={
                'validation_type': validation_type,
                'failed_rules': failed_rules or [],
                'dataset': dataset
            }
        )
        self.validation_type = validation_type
        self.failed_rules = failed_rules or []
        self.dataset = dataset


class SchemaValidationError(DataValidationError):
    """Erreur de validation de schéma"""
    
    def __init__(self, message: str, expected_schema: str = None, 
                 actual_schema: str = None, missing_columns: list = None,
                 extra_columns: list = None):
        super().__init__(
            message=message,
            validation_type="schema_validation",
            dataset="unknown"
        )
        self.context.update({
            'expected_schema': expected_schema,
            'actual_schema': actual_schema,
            'missing_columns': missing_columns or [],
            'extra_columns': extra_columns or []
        })
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema
        self.missing_columns = missing_columns or []
        self.extra_columns = extra_columns or []


class DataQualityError(DataValidationError):
    """Erreur de qualité des données"""
    
    def __init__(self, message: str, quality_score: float = None,
                 quality_issues: list = None, dataset: str = None):
        super().__init__(
            message=message,
            validation_type="data_quality",
            dataset=dataset
        )
        self.context.update({
            'quality_score': quality_score,
            'quality_issues': quality_issues or []
        })
        self.quality_score = quality_score
        self.quality_issues = quality_issues or []


class SparkJobError(LakehouseException):
    """Erreur d'exécution de job Spark"""
    
    def __init__(self, message: str, job_name: str = None, stage: str = None,
                 spark_error: str = None, job_id: str = None):
        super().__init__(
            message=message,
            error_code="SPARK_JOB_ERROR",
            context={
                'job_name': job_name,
                'stage': stage,
                'spark_error': spark_error,
                'job_id': job_id
            }
        )
        self.job_name = job_name
        self.stage = stage
        self.spark_error = spark_error
        self.job_id = job_id


class DatabaseConnectionError(LakehouseException):
    """Erreur de connexion à la base de données"""
    
    def __init__(self, message: str, database_type: str = None,
                 host: str = None, port: int = None, database: str = None):
        super().__init__(
            message=message,
            error_code="DATABASE_CONNECTION_ERROR",
            context={
                'database_type': database_type,
                'host': host,
                'port': port,
                'database': database
            }
        )
        self.database_type = database_type
        self.host = host
        self.port = port
        self.database = database


class DatabaseOperationError(LakehouseException):
    """Erreur d'opération de base de données"""
    
    def __init__(self, message: str, operation: str = None, table: str = None,
                 sql_error: str = None, rows_affected: int = None):
        super().__init__(
            message=message,
            error_code="DATABASE_OPERATION_ERROR",
            context={
                'operation': operation,
                'table': table,
                'sql_error': sql_error,
                'rows_affected': rows_affected
            }
        )
        self.operation = operation
        self.table = table
        self.sql_error = sql_error
        self.rows_affected = rows_affected


class FileProcessingError(LakehouseException):
    """Erreur de traitement de fichier"""
    
    def __init__(self, message: str, file_path: str = None, file_format: str = None,
                 operation: str = None, file_size: int = None):
        super().__init__(
            message=message,
            error_code="FILE_PROCESSING_ERROR",
            context={
                'file_path': file_path,
                'file_format': file_format,
                'operation': operation,
                'file_size': file_size
            }
        )
        self.file_path = file_path
        self.file_format = file_format
        self.operation = operation
        self.file_size = file_size


class APIError(LakehouseException):
    """Erreur d'API"""
    
    def __init__(self, message: str, status_code: int = None, endpoint: str = None,
                 method: str = None, request_data: Dict[str, Any] = None):
        super().__init__(
            message=message,
            error_code="API_ERROR",
            context={
                'status_code': status_code,
                'endpoint': endpoint,
                'method': method,
                'request_data': request_data
            }
        )
        self.status_code = status_code
        self.endpoint = endpoint
        self.method = method
        self.request_data = request_data


class MLModelError(LakehouseException):
    """Erreur de modèle ML"""
    
    def __init__(self, message: str, model_name: str = None, model_version: str = None,
                 operation: str = None, metrics: Dict[str, Any] = None):
        super().__init__(
            message=message,
            error_code="ML_MODEL_ERROR",
            context={
                'model_name': model_name,
                'model_version': model_version,
                'operation': operation,
                'metrics': metrics
            }
        )
        self.model_name = model_name
        self.model_version = model_version
        self.operation = operation
        self.metrics = metrics


class ResourceError(LakehouseException):
    """Erreur de ressource système"""
    
    def __init__(self, message: str, resource_type: str = None, 
                 resource_limit: str = None, current_usage: str = None):
        super().__init__(
            message=message,
            error_code="RESOURCE_ERROR",
            context={
                'resource_type': resource_type,
                'resource_limit': resource_limit,
                'current_usage': current_usage
            }
        )
        self.resource_type = resource_type
        self.resource_limit = resource_limit
        self.current_usage = current_usage


class BusinessLogicError(LakehouseException):
    """Erreur de logique métier"""
    
    def __init__(self, message: str, business_rule: str = None,
                 entity_type: str = None, entity_id: str = None):
        super().__init__(
            message=message,
            error_code="BUSINESS_LOGIC_ERROR",
            context={
                'business_rule': business_rule,
                'entity_type': entity_type,
                'entity_id': entity_id
            }
        )
        self.business_rule = business_rule
        self.entity_type = entity_type
        self.entity_id = entity_id