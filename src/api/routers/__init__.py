"""
Routers FastAPI pour l'API
"""

from .accidents import router as accidents_router
from .kpis import router as kpis_router
from .hotspots import router as hotspots_router
from .predictions import router as predictions_router

__all__ = [
    "accidents_router",
    "kpis_router",
    "hotspots_router",
    "predictions_router"
]