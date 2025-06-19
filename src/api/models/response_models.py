"""
Modèles Pydantic pour les réponses génériques
"""

from datetime import datetime
from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field
from enum import Enum


class ResponseStatus(str, Enum):
    """Statuts de réponse possibles"""
    success = "success"
    error = "error"
    warning = "warning"
    partial = "partial"


class ErrorType(str, Enum):
    """Types d'erreurs"""
    validation_error = "validation_error"
    not_found = "not_found"
    internal_error = "internal_error"
    authentication_error = "authentication_error"
    authorization_error = "authorization_error"
    rate_limit_error = "rate_limit_error"
    service_unavailable = "service_unavailable"


class APIResponse(BaseModel):
    """Modèle de réponse générique pour l'API"""
    status: ResponseStatus = Field(..., description="Statut de la réponse")
    message: str = Field(..., description="Message descriptif")
    data: Optional[Union[Dict[str, Any], List[Any]]] = Field(None, description="Données de la réponse")
    timestamp: datetime = Field(default_factory=datetime.now, description="Horodatage de la réponse")
    request_id: Optional[str] = Field(None, description="Identifiant unique de la requête")
    
    class Config:
        schema_extra = {
            "example": {
                "status": "success",
                "message": "Données récupérées avec succès",
                "data": {"key": "value"},
                "timestamp": "2023-12-01T10:30:00Z",
                "request_id": "req-123456"
            }
        }


class ErrorResponse(BaseModel):
    """Modèle de réponse d'erreur"""
    status: ResponseStatus = Field(ResponseStatus.error, description="Statut d'erreur")
    error_type: ErrorType = Field(..., description="Type d'erreur")
    message: str = Field(..., description="Message d'erreur")
    details: Optional[Dict[str, Any]] = Field(None, description="Détails de l'erreur")
    timestamp: datetime = Field(default_factory=datetime.now, description="Horodatage de l'erreur")
    request_id: Optional[str] = Field(None, description="Identifiant de la requête")
    trace_id: Optional[str] = Field(None, description="Identifiant de trace pour le debugging")
    
    class Config:
        schema_extra = {
            "example": {
                "status": "error",
                "error_type": "validation_error",
                "message": "Données d'entrée invalides",
                "details": {
                    "field": "severity",
                    "error": "La valeur doit être entre 1 et 4"
                },
                "timestamp": "2023-12-01T10:30:00Z",
                "request_id": "req-123456",
                "trace_id": "trace-789"
            }
        }


class ValidationErrorResponse(ErrorResponse):
    """Modèle de réponse pour les erreurs de validation"""
    error_type: ErrorType = Field(ErrorType.validation_error, description="Type d'erreur de validation")
    validation_errors: List[Dict[str, str]] = Field(..., description="Liste des erreurs de validation")
    
    class Config:
        schema_extra = {
            "example": {
                "status": "error",
                "error_type": "validation_error",
                "message": "Erreurs de validation des données",
                "validation_errors": [
                    {
                        "field": "severity",
                        "error": "La valeur doit être entre 1 et 4"
                    },
                    {
                        "field": "state",
                        "error": "Code état requis"
                    }
                ],
                "timestamp": "2023-12-01T10:30:00Z"
            }
        }


class HealthCheckResponse(BaseModel):
    """Modèle de réponse pour le health check"""
    status: str = Field(..., description="Statut de santé (healthy/unhealthy)")
    service: str = Field(..., description="Nom du service")
    version: str = Field(..., description="Version du service")
    timestamp: datetime = Field(default_factory=datetime.now, description="Horodatage du check")
    uptime_seconds: float = Field(..., description="Temps de fonctionnement en secondes")
    dependencies: Dict[str, str] = Field(..., description="Statut des dépendances")
    metrics: Optional[Dict[str, Any]] = Field(None, description="Métriques de performance")
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "service": "lakehouse-api",
                "version": "1.0.0",
                "timestamp": "2023-12-01T10:30:00Z",
                "uptime_seconds": 3600.5,
                "dependencies": {
                    "mysql": "healthy",
                    "mlflow": "healthy",
                    "redis": "healthy"
                },
                "metrics": {
                    "requests_per_minute": 45.2,
                    "avg_response_time_ms": 125.3,
                    "error_rate_percent": 0.1
                }
            }
        }


class PaginationMeta(BaseModel):
    """Métadonnées de pagination"""
    page: int = Field(..., ge=1, description="Page actuelle")
    size: int = Field(..., ge=1, description="Taille de la page")
    total_pages: int = Field(..., ge=0, description="Nombre total de pages")
    total_items: int = Field(..., ge=0, description="Nombre total d'éléments")
    has_next: bool = Field(..., description="Page suivante disponible")
    has_previous: bool = Field(..., description="Page précédente disponible")
    
    class Config:
        schema_extra = {
            "example": {
                "page": 1,
                "size": 20,
                "total_pages": 50,
                "total_items": 1000,
                "has_next": True,
                "has_previous": False
            }
        }


class PaginatedResponse(BaseModel):
    """Modèle de réponse paginée générique"""
    data: List[Any] = Field(..., description="Données de la page")
    pagination: PaginationMeta = Field(..., description="Métadonnées de pagination")
    filters: Optional[Dict[str, Any]] = Field(None, description="Filtres appliqués")
    sort: Optional[Dict[str, str]] = Field(None, description="Tri appliqué")
    timestamp: datetime = Field(default_factory=datetime.now, description="Horodatage de la réponse")
    
    class Config:
        schema_extra = {
            "example": {
                "data": [{"id": 1, "name": "Item 1"}],
                "pagination": {
                    "page": 1,
                    "size": 20,
                    "total_pages": 50,
                    "total_items": 1000,
                    "has_next": True,
                    "has_previous": False
                },
                "filters": {"state": "CA"},
                "sort": {"field": "date", "order": "desc"},
                "timestamp": "2023-12-01T10:30:00Z"
            }
        }


class MetricsResponse(BaseModel):
    """Modèle de réponse pour les métriques"""
    service: str = Field(..., description="Nom du service")
    metrics: Dict[str, Union[int, float, str]] = Field(..., description="Métriques collectées")
    period: str = Field(..., description="Période des métriques")
    timestamp: datetime = Field(default_factory=datetime.now, description="Horodatage des métriques")
    
    class Config:
        schema_extra = {
            "example": {
                "service": "lakehouse-api",
                "metrics": {
                    "requests_total": 15420,
                    "requests_per_minute": 45.2,
                    "avg_response_time_ms": 125.3,
                    "error_rate_percent": 0.1,
                    "active_connections": 12
                },
                "period": "last_hour",
                "timestamp": "2023-12-01T10:30:00Z"
            }
        }


class BulkOperationResponse(BaseModel):
    """Modèle de réponse pour les opérations en lot"""
    total_items: int = Field(..., ge=0, description="Nombre total d'éléments traités")
    successful_items: int = Field(..., ge=0, description="Nombre d'éléments traités avec succès")
    failed_items: int = Field(..., ge=0, description="Nombre d'éléments en échec")
    errors: List[Dict[str, Any]] = Field(..., description="Liste des erreurs rencontrées")
    processing_time_ms: float = Field(..., ge=0.0, description="Temps de traitement total")
    timestamp: datetime = Field(default_factory=datetime.now, description="Horodatage de l'opération")
    
    class Config:
        schema_extra = {
            "example": {
                "total_items": 100,
                "successful_items": 95,
                "failed_items": 5,
                "errors": [
                    {
                        "item_id": "item-123",
                        "error": "Validation failed",
                        "details": "Invalid state code"
                    }
                ],
                "processing_time_ms": 1250.5,
                "timestamp": "2023-12-01T10:30:00Z"
            }
        }


class CacheInfo(BaseModel):
    """Informations sur le cache"""
    cached: bool = Field(..., description="Données servies depuis le cache")
    cache_key: Optional[str] = Field(None, description="Clé de cache utilisée")
    cache_ttl_seconds: Optional[int] = Field(None, description="TTL du cache en secondes")
    cache_hit_rate: Optional[float] = Field(None, description="Taux de hit du cache")
    
    class Config:
        schema_extra = {
            "example": {
                "cached": True,
                "cache_key": "accidents:CA:page1",
                "cache_ttl_seconds": 300,
                "cache_hit_rate": 0.85
            }
        }


class APIInfo(BaseModel):
    """Informations sur l'API"""
    name: str = Field(..., description="Nom de l'API")
    version: str = Field(..., description="Version de l'API")
    description: str = Field(..., description="Description de l'API")
    documentation_url: str = Field(..., description="URL de la documentation")
    contact: Dict[str, str] = Field(..., description="Informations de contact")
    license: Dict[str, str] = Field(..., description="Informations de licence")
    endpoints_count: int = Field(..., description="Nombre d'endpoints disponibles")
    
    class Config:
        schema_extra = {
            "example": {
                "name": "Lakehouse Accidents US API",
                "version": "1.0.0",
                "description": "API d'analyse des accidents de la route aux États-Unis",
                "documentation_url": "/docs",
                "contact": {
                    "name": "Support API",
                    "email": "support@example.com"
                },
                "license": {
                    "name": "MIT",
                    "url": "https://opensource.org/licenses/MIT"
                },
                "endpoints_count": 12
            }
        }


class RateLimitInfo(BaseModel):
    """Informations sur les limites de taux"""
    limit: int = Field(..., description="Limite de requêtes")
    remaining: int = Field(..., description="Requêtes restantes")
    reset_time: datetime = Field(..., description="Heure de reset de la limite")
    window_seconds: int = Field(..., description="Fenêtre de temps en secondes")
    
    class Config:
        schema_extra = {
            "example": {
                "limit": 1000,
                "remaining": 850,
                "reset_time": "2023-12-01T11:00:00Z",
                "window_seconds": 3600
            }
        }