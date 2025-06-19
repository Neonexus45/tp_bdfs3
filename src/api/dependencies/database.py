"""
Dépendances pour la base de données (version simplifiée)
"""

from typing import Generator
from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, status

from ...common.config.config_manager import ConfigManager
from ...common.utils.database_connector import DatabaseConnector
from ...common.utils.logger import Logger


# Instance globale du connecteur de base de données
_db_connector = None
_config_manager = None
_logger = None


def get_config_manager() -> ConfigManager:
    """Récupère l'instance du gestionnaire de configuration"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager


def get_logger() -> Logger:
    """Récupère l'instance du logger"""
    global _logger
    if _logger is None:
        config_manager = get_config_manager()
        _logger = Logger("api_database", config_manager)
    return _logger


def get_database_connector() -> DatabaseConnector:
    """Récupère l'instance du connecteur de base de données"""
    global _db_connector
    if _db_connector is None:
        config_manager = get_config_manager()
        _db_connector = DatabaseConnector(config_manager)
        
        # Test de la connexion au démarrage
        logger = get_logger()
        if not _db_connector.test_connection():
            logger.error("Database connection failed during startup")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Service de base de données indisponible"
            )
        
        logger.info("Database connector initialized successfully")
    
    return _db_connector


def get_db_session() -> Generator[Session, None, None]:
    """
    Dépendance FastAPI pour obtenir une session de base de données
    
    Yields:
        Session: Session SQLAlchemy
    """
    db_connector = get_database_connector()
    logger = get_logger()
    
    try:
        with db_connector.get_session() as session:
            logger.debug("Database session created")
            yield session
    except Exception as e:
        logger.error("Database session error", exception=e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Erreur de connexion à la base de données"
        )
    finally:
        logger.debug("Database session closed")


def get_db_connection():
    """
    Dépendance FastAPI pour obtenir une connexion directe à la base de données
    
    Yields:
        Connection: Connexion SQLAlchemy
    """
    db_connector = get_database_connector()
    logger = get_logger()
    
    try:
        with db_connector.get_connection() as connection:
            logger.debug("Database connection created")
            yield connection
    except Exception as e:
        logger.error("Database connection error", exception=e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Erreur de connexion à la base de données"
        )
    finally:
        logger.debug("Database connection closed")


async def check_database_health() -> dict:
    """
    Vérifie la santé de la base de données
    
    Returns:
        dict: Informations sur la santé de la base de données
    """
    logger = get_logger()
    
    try:
        db_connector = get_database_connector()
        
        # Test de connexion
        is_connected = db_connector.test_connection()
        
        # Statistiques de connexion
        connection_stats = db_connector.get_connection_stats()
        
        health_info = {
            "status": "healthy" if is_connected else "unhealthy",
            "connected": is_connected,
            "connection_stats": connection_stats,
            "engine_info": {
                "pool_size": db_connector.engine.pool.size(),
                "checked_in": db_connector.engine.pool.checkedin(),
                "checked_out": db_connector.engine.pool.checkedout(),
                "overflow": db_connector.engine.pool.overflow(),
                "invalid": db_connector.engine.pool.invalid()
            }
        }
        
        logger.info("Database health check completed", health_status=health_info["status"])
        return health_info
        
    except Exception as e:
        logger.error("Database health check failed", exception=e)
        return {
            "status": "unhealthy",
            "connected": False,
            "error": str(e)
        }


def execute_query_with_pagination(
    query: str,
    params: dict = None,
    page: int = 1,
    size: int = 20,
    db_connector: DatabaseConnector = None
) -> dict:
    """
    Exécute une requête avec pagination
    
    Args:
        query: Requête SQL
        params: Paramètres de la requête
        page: Numéro de page
        size: Taille de la page
        db_connector: Connecteur de base de données
    
    Returns:
        dict: Résultats paginés
    """
    if db_connector is None:
        db_connector = get_database_connector()
    
    logger = get_logger()
    
    try:
        # Calcul de l'offset
        offset = (page - 1) * size
        
        # Requête pour compter le total
        count_query = f"SELECT COUNT(*) as total FROM ({query}) as count_subquery"
        count_result = db_connector.execute_query(count_query, params)
        total_items = count_result[0]['total'] if count_result else 0
        
        # Requête paginée
        paginated_query = f"{query} LIMIT {size} OFFSET {offset}"
        data = db_connector.execute_query(paginated_query, params)
        
        # Calcul des métadonnées de pagination
        total_pages = (total_items + size - 1) // size
        has_next = page < total_pages
        has_previous = page > 1
        
        result = {
            "data": data,
            "pagination": {
                "page": page,
                "size": size,
                "total_pages": total_pages,
                "total_items": total_items,
                "has_next": has_next,
                "has_previous": has_previous
            }
        }
        
        logger.debug("Paginated query executed", 
                    page=page, 
                    size=size, 
                    total_items=total_items,
                    results_count=len(data))
        
        return result
        
    except Exception as e:
        logger.error("Paginated query execution failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de l'exécution de la requête"
        )


def build_where_clause(filters: dict) -> tuple[str, dict]:
    """
    Construit une clause WHERE à partir des filtres
    
    Args:
        filters: Dictionnaire des filtres
    
    Returns:
        tuple: (clause WHERE, paramètres)
    """
    if not filters:
        return "", {}
    
    conditions = []
    params = {}
    param_counter = 1
    
    for field, value in filters.items():
        if value is not None:
            param_name = f"param_{param_counter}"
            
            if isinstance(value, str) and '%' in value:
                conditions.append(f"{field} LIKE %({param_name})s")
            elif isinstance(value, list):
                placeholders = []
                for i, item in enumerate(value):
                    item_param = f"{param_name}_{i}"
                    placeholders.append(f"%({item_param})s")
                    params[item_param] = item
                conditions.append(f"{field} IN ({', '.join(placeholders)})")
                param_counter += len(value) - 1
            else:
                conditions.append(f"{field} = %({param_name})s")
            
            if not isinstance(value, list):
                params[param_name] = value
            param_counter += 1
    
    where_clause = " AND ".join(conditions)
    return f"WHERE {where_clause}" if where_clause else "", params


def build_order_clause(sort_field: str = None, sort_order: str = "desc") -> str:
    """
    Construit une clause ORDER BY
    
    Args:
        sort_field: Champ de tri
        sort_order: Ordre de tri (asc/desc)
    
    Returns:
        str: Clause ORDER BY
    """
    if not sort_field:
        return "ORDER BY id DESC"  # Tri par défaut
    
    # Validation de l'ordre de tri
    if sort_order.lower() not in ['asc', 'desc']:
        sort_order = 'desc'
    
    # Validation basique du nom de champ (sécurité)
    allowed_fields = [
        'id', 'severity', 'start_time', 'end_time', 'distance_mi',
        'temperature_f', 'humidity_percent', 'visibility_mi', 'state',
        'city', 'county', 'accident_count', 'avg_severity', 'created_at'
    ]
    
    if sort_field not in allowed_fields:
        sort_field = 'id'
    
    return f"ORDER BY {sort_field} {sort_order.upper()}"


# Dépendances FastAPI simplifiées
DatabaseSession = Depends(get_db_session)
DatabaseConnection = Depends(get_db_connection)
DatabaseConnectorDep = Depends(get_database_connector)