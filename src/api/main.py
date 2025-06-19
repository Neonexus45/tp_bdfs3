"""
Application FastAPI principale pour l'API d'analyse des accidents US
"""

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from ..common.config.config_manager import ConfigManager
from ..common.utils.logger import Logger
from .middleware.cors_middleware import setup_cors_middleware
from .middleware.logging_middleware import LoggingMiddleware
from .routers import (
    accidents_router,
    kpis_router,
    hotspots_router,
    predictions_router
)
from .dependencies.database import check_database_health


def create_app() -> FastAPI:
    """Crée l'application FastAPI"""
    config_manager = ConfigManager()
    api_config = config_manager.get('api')
    logger = Logger("api_main", config_manager)
    
    app = FastAPI(
        title="API Analyse Accidents US",
        description="API REST pour l'analyse des accidents de la route aux États-Unis",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # Configuration CORS
    setup_cors_middleware(app, config_manager)
    
    # Middleware de logging
    app.add_middleware(LoggingMiddleware, config_manager=config_manager)
    
    # Inclusion des routers
    app.include_router(accidents_router, prefix="/api/v1")
    app.include_router(kpis_router, prefix="/api/v1")
    app.include_router(hotspots_router, prefix="/api/v1")
    app.include_router(predictions_router, prefix="/api/v1")
    
    logger.log_startup(
        app_name="lakehouse-api",
        version="1.0.0",
        config_summary={
            'host': api_config.get('host', '0.0.0.0'),
            'port': api_config.get('port', 8000),
            'cors_enabled': True,
            'authentication': False  # Pas d'authentification pour le projet scolaire
        }
    )
    
    return app


app = create_app()


@app.get("/")
async def root():
    """Endpoint racine"""
    return {
        "message": "API d'Analyse des Accidents US",
        "status": "running",
        "version": "1.0.0",
        "documentation": "/docs",
        "endpoints": {
            "accidents": "/api/v1/accidents",
            "kpis": "/api/v1/kpis",
            "hotspots": "/api/v1/hotspots",
            "predictions": "/api/v1/predict"
        }
    }


@app.get("/health")
async def health_check():
    """Endpoint de santé avec vérification de la base de données"""
    try:
        # Vérification de la base de données
        db_health = await check_database_health()
        
        overall_status = "healthy" if db_health["status"] == "healthy" else "unhealthy"
        
        return {
            "status": overall_status,
            "service": "lakehouse-api",
            "version": "1.0.0",
            "components": {
                "database": db_health["status"],
                "api": "healthy"
            },
            "database_details": db_health
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "service": "lakehouse-api",
                "error": str(e)
            }
        )


@app.get("/info")
async def api_info():
    """Informations détaillées sur l'API"""
    return {
        "name": "API d'Analyse des Accidents US",
        "description": "API REST pour l'analyse des données d'accidents de la route aux États-Unis",
        "version": "1.0.0",
        "features": [
            "Consultation des données d'accidents",
            "KPIs de sécurité routière",
            "Identification des zones dangereuses (hotspots)",
            "Prédictions ML de sévérité d'accidents"
        ],
        "data_sources": [
            "Couche Gold MySQL (accidents_summary)",
            "Tables KPIs optimisées",
            "Modèles ML via MLflow"
        ],
        "endpoints": {
            "accidents": {
                "base": "/api/v1/accidents",
                "description": "Données d'accidents avec filtres et pagination"
            },
            "kpis": {
                "base": "/api/v1/kpis",
                "description": "Indicateurs de performance clés"
            },
            "hotspots": {
                "base": "/api/v1/hotspots",
                "description": "Zones dangereuses géolocalisées"
            },
            "predictions": {
                "base": "/api/v1/predict",
                "description": "Prédictions ML de sévérité"
            }
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json"
        }
    }


def main():
    """Point d'entrée principal"""
    import uvicorn
    config_manager = ConfigManager()
    api_config = config_manager.get('api')
    
    uvicorn.run(
        "src.api.main:app",
        host=api_config.get('host', '0.0.0.0'),
        port=api_config.get('port', 8000),
        reload=config_manager.get('debug', False)
    )


if __name__ == "__main__":
    main()