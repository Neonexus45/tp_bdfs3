"""
Modèles Pydantic pour l'API FastAPI
"""

# Modèles d'accidents
from .accident_models import (
    AccidentBase,
    AccidentResponse,
    AccidentDetailResponse,
    AccidentListResponse,
    AccidentFilters,
    AccidentSortOptions,
    PaginationInfo
)

# Modèles de KPIs
from .kpi_models import (
    KPIMetricType,
    TrendDirection,
    PeriodType,
    KPISecurityResponse,
    KPITemporalResponse,
    KPIInfrastructureResponse,
    KPIMLPerformanceResponse,
    KPIListResponse,
    KPIFilters,
    KPISummaryResponse
)

# Modèles de prédictions
from .prediction_models import (
    PredictionRequest,
    PredictionResponse,
    BatchPredictionRequest,
    BatchPredictionResponse,
    ModelInfo,
    ModelsListResponse
)

# Modèles de hotspots
from .hotspot_models import (
    HotspotBase,
    HotspotResponse,
    HotspotDetailResponse,
    HotspotListResponse,
    HotspotFilters,
    HotspotAnalysis,
    NearbyHotspotsRequest,
    NearbyHotspotsResponse
)

# Modèles de réponses génériques
from .response_models import (
    ResponseStatus,
    ErrorType,
    APIResponse,
    ErrorResponse,
    ValidationErrorResponse,
    HealthCheckResponse,
    PaginationMeta,
    PaginatedResponse,
    MetricsResponse,
    BulkOperationResponse,
    CacheInfo,
    APIInfo,
    RateLimitInfo
)

__all__ = [
    # Accidents
    "AccidentBase",
    "AccidentResponse", 
    "AccidentDetailResponse",
    "AccidentListResponse",
    "AccidentFilters",
    "AccidentSortOptions",
    "PaginationInfo",
    
    # KPIs
    "KPIMetricType",
    "TrendDirection",
    "PeriodType",
    "KPISecurityResponse",
    "KPITemporalResponse",
    "KPIInfrastructureResponse",
    "KPIMLPerformanceResponse",
    "KPIListResponse",
    "KPIFilters",
    "KPISummaryResponse",
    
    # Prédictions
    "PredictionRequest",
    "PredictionResponse",
    "BatchPredictionRequest",
    "BatchPredictionResponse",
    "ModelInfo",
    "ModelsListResponse",
    
    # Hotspots
    "HotspotBase",
    "HotspotResponse",
    "HotspotDetailResponse",
    "HotspotListResponse",
    "HotspotFilters",
    "HotspotAnalysis",
    "NearbyHotspotsRequest",
    "NearbyHotspotsResponse",
    
    # Réponses génériques
    "ResponseStatus",
    "ErrorType",
    "APIResponse",
    "ErrorResponse",
    "ValidationErrorResponse",
    "HealthCheckResponse",
    "PaginationMeta",
    "PaginatedResponse",
    "MetricsResponse",
    "BulkOperationResponse",
    "CacheInfo",
    "APIInfo",
    "RateLimitInfo"
]