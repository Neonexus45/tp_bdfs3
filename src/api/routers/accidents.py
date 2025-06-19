"""
Router pour les endpoints des accidents (version simplifiée)
"""

from typing import Optional, List
from datetime import date
from fastapi import APIRouter, HTTPException, status, Query, Path
from sqlalchemy.orm import Session

from ..models.accident_models import (
    AccidentResponse, AccidentDetailResponse, AccidentListResponse, 
    AccidentFilters, AccidentSortOptions
)
from ..models.response_models import ErrorResponse
from ..services.accident_service import AccidentService
from ..dependencies.database import DatabaseSession
from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger

# Configuration du router
router = APIRouter(
    prefix="/accidents",
    tags=["Accidents"],
    responses={
        404: {"model": ErrorResponse, "description": "Accident non trouvé"},
        500: {"model": ErrorResponse, "description": "Erreur interne du serveur"}
    }
)

# Instances globales
config_manager = ConfigManager()
logger = Logger("accidents_router", config_manager)
accident_service = AccidentService(config_manager)


@router.get(
    "",
    response_model=AccidentListResponse,
    summary="Liste des accidents",
    description="Récupère une liste paginée d'accidents avec filtres et tri"
)
async def get_accidents(
    page: int = Query(1, ge=1, description="Numéro de page"),
    size: int = Query(20, ge=1, le=100, description="Taille de page"),
    state: Optional[str] = Query(None, description="Code état (ex: CA, TX)"),
    city: Optional[str] = Query(None, description="Ville"),
    severity: Optional[int] = Query(None, ge=1, le=4, description="Niveau sévérité (1-4)"),
    date_from: Optional[date] = Query(None, description="Date début (YYYY-MM-DD)"),
    date_to: Optional[date] = Query(None, description="Date fin (YYYY-MM-DD)"),
    weather_category: Optional[str] = Query(None, description="Catégorie météorologique"),
    min_safety_score: Optional[float] = Query(None, ge=0.0, le=10.0, description="Score sécurité minimum"),
    max_safety_score: Optional[float] = Query(None, ge=0.0, le=10.0, description="Score sécurité maximum"),
    sort_by: str = Query("accident_date", description="Colonne de tri"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Ordre tri"),
    db: Session = DatabaseSession
):
    """Récupère une liste paginée d'accidents avec filtres et tri"""
    try:
        logger.info("GET /accidents requested", 
                   page=page, 
                   size=size,
                   state=state,
                   severity=severity)
        
        # Construction des filtres
        filters = AccidentFilters(
            state=state,
            city=city,
            severity=severity,
            date_from=date_from,
            date_to=date_to,
            weather_category=weather_category,
            min_safety_score=min_safety_score,
            max_safety_score=max_safety_score
        )
        
        # Options de tri
        sort_options = AccidentSortOptions(
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        # Récupération des données
        result = await accident_service.get_accidents(
            page=page,
            size=size,
            filters=filters,
            sort_options=sort_options
        )
        
        response = AccidentListResponse(**result)
        
        logger.info("GET /accidents completed successfully",
                   accidents_count=len(response.accidents),
                   total_items=response.pagination.total_items)
        
        return response
        
    except ValueError as e:
        logger.warning("Invalid parameters for GET /accidents", exception=e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Paramètres invalides: {str(e)}"
        )
    except Exception as e:
        logger.error("GET /accidents failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des accidents"
        )


@router.get(
    "/{accident_id}",
    response_model=AccidentDetailResponse,
    summary="Détails d'un accident",
    description="Récupère les détails complets d'un accident par son ID"
)
async def get_accident_by_id(
    accident_id: str = Path(..., description="Identifiant unique de l'accident"),
    db: Session = DatabaseSession
):
    """Récupère les détails complets d'un accident spécifique"""
    try:
        logger.info("GET /accidents/{accident_id} requested", accident_id=accident_id)
        
        accident = await accident_service.get_accident_by_id(accident_id)
        
        if not accident:
            logger.warning("Accident not found", accident_id=accident_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Accident {accident_id} non trouvé"
            )
        
        response = AccidentDetailResponse(**accident)
        
        logger.info("GET /accidents/{accident_id} completed successfully", accident_id=accident_id)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("GET /accidents/{accident_id} failed", exception=e, accident_id=accident_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération de l'accident"
        )


@router.get(
    "/summary/statistics",
    response_model=dict,
    summary="Résumé statistique des accidents",
    description="Récupère un résumé statistique des accidents avec filtres optionnels"
)
async def get_accidents_summary(
    state: Optional[str] = Query(None, description="Code état pour filtrer"),
    date_from: Optional[date] = Query(None, description="Date début pour filtrer"),
    date_to: Optional[date] = Query(None, description="Date fin pour filtrer"),
    db: Session = DatabaseSession
):
    """Récupère un résumé statistique des accidents"""
    try:
        logger.info("GET /accidents/summary/statistics requested", state=state)
        
        # Construction des filtres
        filters = AccidentFilters(
            state=state,
            date_from=date_from,
            date_to=date_to
        )
        
        summary = await accident_service.get_accidents_summary(filters)
        
        logger.info("GET /accidents/summary/statistics completed successfully",
                   total_accidents=summary.get('total_accidents', 0))
        
        return summary
        
    except Exception as e:
        logger.error("GET /accidents/summary/statistics failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération du résumé"
        )


@router.get(
    "/states/list",
    response_model=List[dict],
    summary="Liste des états",
    description="Récupère la liste des états avec statistiques d'accidents"
)
async def get_states_list(
    db: Session = DatabaseSession
):
    """Récupère la liste de tous les états avec leurs statistiques d'accidents"""
    try:
        logger.info("GET /accidents/states/list requested")
        
        states = await accident_service.get_states_list()
        
        logger.info("GET /accidents/states/list completed successfully", states_count=len(states))
        
        return states
        
    except Exception as e:
        logger.error("GET /accidents/states/list failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération de la liste des états"
        )