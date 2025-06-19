"""
Dépendances FastAPI pour l'API (version simplifiée)
"""

# Dépendances de base de données
from .database import (
    get_config_manager,
    get_database_connector,
    get_db_session,
    get_db_connection,
    check_database_health,
    execute_query_with_pagination,
    DatabaseSession,
    DatabaseConnection,
    DatabaseConnectorDep
)

__all__ = [
    # Base de données
    "get_config_manager",
    "get_database_connector",
    "get_db_session",
    "get_db_connection",
    "check_database_health",
    "execute_query_with_pagination",
    "DatabaseSession",
    "DatabaseConnection",
    "DatabaseConnectorDep"
]