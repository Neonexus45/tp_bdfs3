"""
Middleware CORS simple pour l'API
"""

from fastapi.middleware.cors import CORSMiddleware
from ...common.config.config_manager import ConfigManager


def setup_cors_middleware(app, config_manager: ConfigManager = None):
    """Configure le middleware CORS pour l'application"""
    config_manager = config_manager or ConfigManager()
    
    # Configuration CORS permissive pour le développement
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Permissif pour le développement
        allow_credentials=False,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )