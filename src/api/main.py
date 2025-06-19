from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from ..common.config.config_manager import ConfigManager
from ..common.utils.logger import Logger


def create_app() -> FastAPI:
    """Crée l'application FastAPI"""
    config_manager = ConfigManager()
    api_config = config_manager.get('api')
    logger = Logger("api_main", config_manager)
    
    app = FastAPI(
        title=api_config['title'],
        description=api_config['description'],
        version=api_config['version']
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=api_config['cors']['origins'],
        allow_credentials=api_config['cors']['allow_credentials'],
        allow_methods=api_config['cors']['allow_methods'],
        allow_headers=api_config['cors']['allow_headers'],
    )
    
    logger.log_startup(
        app_name="lakehouse-api",
        version=api_config['version'],
        config_summary={
            'host': api_config['host'],
            'port': api_config['port'],
            'cors_enabled': True
        }
    )
    
    return app


app = create_app()


@app.get("/")
async def root():
    """Endpoint racine"""
    return {"message": "Lakehouse Accidents US API", "status": "running"}


@app.get("/health")
async def health_check():
    """Endpoint de santé"""
    return {"status": "healthy", "service": "lakehouse-api"}


def main():
    """Point d'entrée principal"""
    import uvicorn
    config_manager = ConfigManager()
    api_config = config_manager.get('api')
    
    uvicorn.run(
        "src.api.main:app",
        host=api_config['host'],
        port=api_config['port'],
        reload=config_manager.get('debug', False)
    )


if __name__ == "__main__":
    main()