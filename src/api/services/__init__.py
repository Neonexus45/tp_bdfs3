"""
Services pour l'API FastAPI
"""

from .accident_service import AccidentService
from .kpi_service import KPIService
from .hotspot_service import HotspotService
from .prediction_service import APIPredictionService

__all__ = [
    "AccidentService",
    "KPIService", 
    "HotspotService",
    "APIPredictionService"
]