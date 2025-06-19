import json
import logging
import logging.handlers
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import structlog
from ..config.config_manager import ConfigManager


class Logger:
    """Logger structuré JSON avec niveaux configurables"""
    
    def __init__(self, name: str, config_manager: ConfigManager = None):
        self.name = name
        self.config_manager = config_manager or ConfigManager()
        self.log_config = self.config_manager.get('logging')
        self._logger = self._setup_logger()
    
    def _setup_logger(self) -> structlog.BoundLogger:
        """Configure le logger structuré"""
        log_level = getattr(logging, self.log_config['level'].upper(), logging.INFO)
        
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=log_level,
        )
        
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                self._add_custom_fields,
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        
        logger = structlog.get_logger(self.name)
        
        if self.log_config.get('file_path'):
            self._setup_file_handler(logger)
        
        return logger
    
    def _add_custom_fields(self, logger, method_name, event_dict):
        """Ajoute des champs personnalisés au log"""
        event_dict['application'] = self.name
        event_dict['environment'] = self.config_manager.get('environment')
        event_dict['hostname'] = self._get_hostname()
        return event_dict
    
    def _get_hostname(self) -> str:
        """Récupère le nom d'hôte"""
        import socket
        try:
            return socket.gethostname()
        except Exception:
            return "unknown"
    
    def _setup_file_handler(self, logger):
        """Configure le handler de fichier avec rotation"""
        log_file_path = Path(self.log_config['file_path'])
        log_file_path.mkdir(parents=True, exist_ok=True)
        
        file_path = log_file_path / f"{self.name}.log"
        
        handler = logging.handlers.RotatingFileHandler(
            filename=str(file_path),
            maxBytes=self._parse_size(self.log_config['file_max_size']),
            backupCount=self.log_config['file_backup_count'],
            encoding='utf-8'
        )
        
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
    
    def _parse_size(self, size_str: str) -> int:
        """Parse une taille de fichier (ex: '100MB')"""
        size_str = size_str.upper()
        if size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        else:
            return int(size_str)
    
    def info(self, message: str, **context):
        """Log un message d'information"""
        self._logger.info(message, **context)
    
    def debug(self, message: str, **context):
        """Log un message de debug"""
        self._logger.debug(message, **context)
    
    def warning(self, message: str, **context):
        """Log un message d'avertissement"""
        self._logger.warning(message, **context)
    
    def error(self, message: str, exception: Exception = None, **context):
        """Log un message d'erreur"""
        if exception:
            context['exception_type'] = type(exception).__name__
            context['exception_message'] = str(exception)
        self._logger.error(message, **context)
    
    def critical(self, message: str, exception: Exception = None, **context):
        """Log un message critique"""
        if exception:
            context['exception_type'] = type(exception).__name__
            context['exception_message'] = str(exception)
        self._logger.critical(message, **context)
    
    def log_performance(self, operation: str, duration_seconds: float, **metrics):
        """Log des métriques de performance"""
        self.info(
            "Performance metrics",
            operation=operation,
            duration_seconds=duration_seconds,
            metrics_type="performance",
            **metrics
        )
    
    def log_data_quality(self, dataset: str, quality_metrics: Dict[str, Any]):
        """Log des métriques de qualité des données"""
        self.info(
            "Data quality metrics",
            dataset=dataset,
            metrics_type="data_quality",
            **quality_metrics
        )
    
    def log_business_event(self, event_type: str, event_data: Dict[str, Any]):
        """Log un événement métier"""
        self.info(
            "Business event",
            event_type=event_type,
            metrics_type="business",
            **event_data
        )
    
    def log_spark_job(self, job_name: str, stage: str, **job_metrics):
        """Log des métriques de job Spark"""
        self.info(
            f"Spark job {stage}",
            job_name=job_name,
            stage=stage,
            metrics_type="spark_job",
            **job_metrics
        )
    
    def log_api_request(self, method: str, endpoint: str, status_code: int, 
                       duration_ms: float, **request_data):
        """Log une requête API"""
        self.info(
            "API request",
            method=method,
            endpoint=endpoint,
            status_code=status_code,
            duration_ms=duration_ms,
            metrics_type="api_request",
            **request_data
        )
    
    def log_database_operation(self, operation: str, table: str, 
                              rows_affected: int = None, duration_ms: float = None):
        """Log une opération de base de données"""
        context = {
            "operation": operation,
            "table": table,
            "metrics_type": "database_operation"
        }
        
        if rows_affected is not None:
            context["rows_affected"] = rows_affected
        if duration_ms is not None:
            context["duration_ms"] = duration_ms
        
        self.info("Database operation", **context)
    
    def create_context(self, **context) -> 'Logger':
        """Crée un logger avec un contexte lié"""
        bound_logger = self._logger.bind(**context)
        new_logger = Logger.__new__(Logger)
        new_logger.name = self.name
        new_logger.config_manager = self.config_manager
        new_logger.log_config = self.log_config
        new_logger._logger = bound_logger
        return new_logger
    
    def log_exception(self, exception: Exception, context: Dict[str, Any] = None):
        """Log une exception avec stack trace"""
        context = context or {}
        context.update({
            'exception_type': type(exception).__name__,
            'exception_message': str(exception),
            'metrics_type': 'exception'
        })
        
        self._logger.exception("Exception occurred", **context)
    
    def log_startup(self, app_name: str, version: str, config_summary: Dict[str, Any]):
        """Log le démarrage d'une application"""
        self.info(
            "Application startup",
            app_name=app_name,
            version=version,
            metrics_type="startup",
            **config_summary
        )
    
    def log_shutdown(self, app_name: str, uptime_seconds: float, final_metrics: Dict[str, Any]):
        """Log l'arrêt d'une application"""
        self.info(
            "Application shutdown",
            app_name=app_name,
            uptime_seconds=uptime_seconds,
            metrics_type="shutdown",
            **final_metrics
        )