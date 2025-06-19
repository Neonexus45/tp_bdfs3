"""
Middleware de logging simple pour les requêtes API
"""

import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware simple pour le logging des requêtes API"""
    
    def __init__(self, app, config_manager: ConfigManager = None):
        super().__init__(app)
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("api_requests", self.config_manager)
    
    async def dispatch(self, request: Request, call_next):
        """Traite chaque requête HTTP avec logging simple"""
        start_time = time.time()
        
        # Logging de la requête entrante
        self.logger.info("API Request", 
                        method=request.method,
                        path=request.url.path,
                        query_params=str(request.query_params))
        
        try:
            # Traitement de la requête
            response = await call_next(request)
            
            # Calcul du temps de traitement
            duration = time.time() - start_time
            
            # Logging de la réponse
            self.logger.info("API Response",
                           method=request.method,
                           path=request.url.path,
                           status_code=response.status_code,
                           duration_ms=round(duration * 1000, 2))
            
            return response
            
        except Exception as e:
            # Logging des erreurs
            duration = time.time() - start_time
            self.logger.error("API Error",
                            method=request.method,
                            path=request.url.path,
                            duration_ms=round(duration * 1000, 2),
                            exception=e)
            raise