"""
Router pour les endpoints des KPIs (version simplifiée)
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, status, Query, Path
from sqlalchemy.orm import Session

from ..models.kpi_models import (
    KPIMetricType, KPIListResponse, KPIFilters
)
from ..models.response_models import ErrorResponse
from ..services.kpi_service import KPIService
from ..dependencies.database import DatabaseSession
from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger

# Configuration du router
router = APIRouter(
    prefix="/kpis",
    tags=["KPIs"],
    responses={
        404: {"model": ErrorResponse, "description": "KPI non trouvé"},
        500: {"model": ErrorResponse, "description": "Erreur interne du serveur"}
    }
)

# Instances globales
config_manager = ConfigManager()
logger = Logger("kpis_router", config_manager)
kpi_service = KPIService(config_manager)


@router.get(
    "/{metric}",
    response_model=KPIListResponse,
    summary="KPIs par métrique",
    description="Récupère les KPIs pour un type de métrique spécifique"
)
async def get_kpis_by_metric(
    metric: KPIMetricType = Path(..., description="Type de métrique KPI"),
    state: Optional[str] = Query(None, description="Code état pour filtrer"),
    period: Optional[str] = Query(None, description="Période (hourly, daily, monthly, yearly)"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats"),
    db: Session = DatabaseSession
):
    """Récupère les KPIs pour un type de métrique spécifique"""
    try:
        logger.info("GET /kpis/{metric} requested",
                   metric=metric,
                   state=state,
                   period=period,
                   limit=limit)
        
        # Construction des filtres
        filters = KPIFilters(
            state=state,
            period=period
        )
        
        # Récupération des KPIs
        result = await kpi_service.get_kpis_by_metric(
            metric_type=metric,
            filters=filters,
            limit=limit
        )
        
        response = KPIListResponse(**result)
        
        logger.info("GET /kpis/{metric} completed successfully",
                   metric=metric,
                   data_count=len(response.data))
        
        return response
        
    except ValueError as e:
        logger.warning("Invalid parameters for GET /kpis/{metric}",
                      exception=e,
                      metric=metric)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Paramètres invalides: {str(e)}"
        )
    except Exception as e:
        logger.error("GET /kpis/{metric} failed",
                    exception=e,
                    metric=metric)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des KPIs"
        )


@router.get(
    "/security/detailed",
    response_model=list,
    summary="KPIs de sécurité détaillés",
    description="Récupère les KPIs de sécurité avec détails complets"
)
async def get_security_kpis_detailed(
    state: Optional[str] = Query(None, description="Code état pour filtrer"),
    limit: int = Query(50, ge=1, le=500, description="Nombre maximum de résultats"),
    db: Session = DatabaseSession
):
    """Récupère les KPIs de sécurité avec informations détaillées"""
    try:
        logger.info("GET /kpis/security/detailed requested",
                   state=state,
                   limit=limit)
        
        security_kpis = await kpi_service.get_security_kpis(
            state=state,
            limit=limit
        )
        
        logger.info("GET /kpis/security/detailed completed successfully",
                   kpis_count=len(security_kpis))
        
        return security_kpis
        
    except Exception as e:
        logger.error("GET /kpis/security/detailed failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des KPIs de sécurité"
        )


@router.get(
    "/temporal/patterns",
    response_model=list,
    summary="Patterns temporels",
    description="Récupère les patterns temporels des accidents"
)
async def get_temporal_patterns(
    period: str = Query("monthly", description="Type de période (hourly, daily, monthly, yearly)"),
    state: Optional[str] = Query(None, description="Code état pour filtrer"),
    limit: int = Query(100, ge=1, le=500, description="Nombre maximum de résultats"),
    db: Session = DatabaseSession
):
    """Récupère les patterns temporels des accidents"""
    try:
        logger.info("GET /kpis/temporal/patterns requested",
                   period=period,
                   state=state,
                   limit=limit)
        
        temporal_kpis = await kpi_service.get_temporal_kpis(
            period=period,
            state=state,
            limit=limit
        )
        
        logger.info("GET /kpis/temporal/patterns completed successfully",
                   patterns_count=len(temporal_kpis))
        
        return temporal_kpis
        
    except Exception as e:
        logger.error("GET /kpis/temporal/patterns failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des patterns temporels"
        )


@router.get(
    "/infrastructure/impact",
    response_model=list,
    summary="Impact des infrastructures",
    description="Récupère l'impact des infrastructures sur la sécurité routière"
)
async def get_infrastructure_impact(
    state: Optional[str] = Query(None, description="Code état pour filtrer"),
    limit: int = Query(50, ge=1, le=200, description="Nombre maximum de résultats"),
    db: Session = DatabaseSession
):
    """Récupère l'impact des infrastructures sur la sécurité routière"""
    try:
        logger.info("GET /kpis/infrastructure/impact requested",
                   state=state,
                   limit=limit)
        
        infrastructure_kpis = await kpi_service.get_infrastructure_kpis(
            state=state,
            limit=limit
        )
        
        logger.info("GET /kpis/infrastructure/impact completed successfully",
                   infrastructure_count=len(infrastructure_kpis))
        
        return infrastructure_kpis
        
    except Exception as e:
        logger.error("GET /kpis/infrastructure/impact failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des KPIs d'infrastructure"
        )


@router.get(
    "/ml-performance/models",
    response_model=list,
    summary="Performance des modèles ML",
    description="Récupère les métriques de performance des modèles de machine learning"
)
async def get_ml_performance_metrics(
    model_name: Optional[str] = Query(None, description="Nom du modèle pour filtrer"),
    limit: int = Query(10, ge=1, le=50, description="Nombre maximum de modèles"),
    db: Session = DatabaseSession
):
    """Récupère les métriques de performance des modèles ML"""
    try:
        logger.info("GET /kpis/ml-performance/models requested",
                   model_name=model_name,
                   limit=limit)
        
        ml_kpis = await kpi_service.get_ml_performance_kpis(
            model_name=model_name,
            limit=limit
        )
        
        logger.info("GET /kpis/ml-performance/models completed successfully",
                   models_count=len(ml_kpis))
        
        return ml_kpis
        
    except Exception as e:
        logger.error("GET /kpis/ml-performance/models failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des KPIs ML"
        )


@router.get(
    "/summary/dashboard",
    response_model=dict,
    summary="Résumé pour tableau de bord",
    description="Récupère un résumé global des KPIs pour tableau de bord"
)
async def get_dashboard_summary(
    db: Session = DatabaseSession
):
    """Récupère un résumé global des KPIs pour affichage en tableau de bord"""
    try:
        logger.info("GET /kpis/summary/dashboard requested")
        
        # Récupération des KPIs de sécurité pour le résumé
        security_kpis = await kpi_service.get_security_kpis(limit=50)
        
        if not security_kpis:
            summary = {
                "total_accidents": 0,
                "avg_severity": 0.0,
                "most_dangerous_state": "N/A",
                "safest_state": "N/A",
                "trend_overall": "stable",
                "last_updated": None,
                "data_quality_score": 0.0
            }
        else:
            # Calcul du résumé
            total_accidents = sum(kpi['total_accidents'] for kpi in security_kpis)
            avg_severity = sum(kpi['avg_severity'] for kpi in security_kpis) / len(security_kpis)
            
            most_dangerous = max(security_kpis, key=lambda x: x['danger_index'])
            safest = min(security_kpis, key=lambda x: x['danger_index'])
            
            summary = {
                "total_accidents": total_accidents,
                "avg_severity": round(avg_severity, 2),
                "most_dangerous_state": most_dangerous['state'],
                "safest_state": safest['state'],
                "trend_overall": "stable",
                "last_updated": max(kpi['last_updated'] for kpi in security_kpis),
                "data_quality_score": 0.95
            }
        
        logger.info("GET /kpis/summary/dashboard completed successfully",
                   total_accidents=summary['total_accidents'])
        
        return summary
        
    except Exception as e:
        logger.error("GET /kpis/summary/dashboard failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération du résumé"
        )