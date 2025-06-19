"""
Router pour les endpoints des hotspots (version simplifiée)
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, status, Query, Path
from sqlalchemy.orm import Session

from ..models.hotspot_models import (
    HotspotResponse, HotspotDetailResponse, HotspotListResponse, 
    HotspotFilters, HotspotAnalysis, NearbyHotspotsRequest, NearbyHotspotsResponse
)
from ..models.response_models import ErrorResponse
from ..services.hotspot_service import HotspotService
from ..dependencies.database import DatabaseSession
from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger

# Configuration du router
router = APIRouter(
    prefix="/hotspots",
    tags=["Hotspots"],
    responses={
        404: {"model": ErrorResponse, "description": "Hotspot non trouvé"},
        500: {"model": ErrorResponse, "description": "Erreur interne du serveur"}
    }
)

# Instances globales
config_manager = ConfigManager()
logger = Logger("hotspots_router", config_manager)
hotspot_service = HotspotService(config_manager)


@router.get(
    "",
    response_model=HotspotListResponse,
    summary="Liste des hotspots",
    description="Récupère une liste de zones dangereuses (hotspots) avec filtres"
)
async def get_hotspots(
    state: Optional[str] = Query(None, description="Code état pour filtrer"),
    city: Optional[str] = Query(None, description="Ville pour filtrer"),
    min_danger_score: Optional[float] = Query(None, ge=0.0, le=10.0, description="Score de danger minimum"),
    max_danger_score: Optional[float] = Query(None, ge=0.0, le=10.0, description="Score de danger maximum"),
    risk_level: Optional[str] = Query(None, description="Niveau de risque (Low, Medium, High, Critical)"),
    min_accident_count: Optional[int] = Query(None, ge=0, description="Nombre minimum d'accidents"),
    center_lat: Optional[float] = Query(None, ge=-90.0, le=90.0, description="Latitude centre recherche"),
    center_lng: Optional[float] = Query(None, ge=-180.0, le=180.0, description="Longitude centre recherche"),
    search_radius_miles: Optional[float] = Query(None, ge=0.0, description="Rayon de recherche en miles"),
    limit: int = Query(50, ge=1, le=100, description="Nombre maximum de hotspots"),
    db: Session = DatabaseSession
):
    """Récupère une liste de zones dangereuses (hotspots) avec possibilité de filtrage"""
    try:
        logger.info("GET /hotspots requested",
                   state=state,
                   min_danger_score=min_danger_score,
                   limit=limit)
        
        # Construction des filtres
        filters = HotspotFilters(
            state=state,
            city=city,
            min_danger_score=min_danger_score,
            max_danger_score=max_danger_score,
            risk_level=risk_level,
            min_accident_count=min_accident_count,
            center_lat=center_lat,
            center_lng=center_lng,
            search_radius_miles=search_radius_miles
        )
        
        # Récupération des hotspots
        result = await hotspot_service.get_hotspots(
            filters=filters,
            limit=limit
        )
        
        response = HotspotListResponse(**result)
        
        logger.info("GET /hotspots completed successfully",
                   hotspots_count=len(response.hotspots),
                   total_hotspots=response.total_hotspots)
        
        return response
        
    except ValueError as e:
        logger.warning("Invalid parameters for GET /hotspots", exception=e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Paramètres invalides: {str(e)}"
        )
    except Exception as e:
        logger.error("GET /hotspots failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des hotspots"
        )


@router.get(
    "/{hotspot_id}",
    response_model=HotspotDetailResponse,
    summary="Détails d'un hotspot",
    description="Récupère les détails complets d'un hotspot par son ID"
)
async def get_hotspot_by_id(
    hotspot_id: str = Path(..., description="Identifiant unique du hotspot"),
    db: Session = DatabaseSession
):
    """Récupère les détails complets d'un hotspot spécifique"""
    try:
        logger.info("GET /hotspots/{hotspot_id} requested", hotspot_id=hotspot_id)
        
        hotspot = await hotspot_service.get_hotspot_by_id(hotspot_id)
        
        if not hotspot:
            logger.warning("Hotspot not found", hotspot_id=hotspot_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Hotspot {hotspot_id} non trouvé"
            )
        
        response = HotspotDetailResponse(**hotspot)
        
        logger.info("GET /hotspots/{hotspot_id} completed successfully",
                   hotspot_id=hotspot_id,
                   danger_score=response.danger_score)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("GET /hotspots/{hotspot_id} failed",
                    exception=e,
                    hotspot_id=hotspot_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération du hotspot"
        )


@router.post(
    "/nearby",
    response_model=NearbyHotspotsResponse,
    summary="Hotspots à proximité",
    description="Trouve les hotspots à proximité d'un point géographique"
)
async def get_nearby_hotspots(
    request: NearbyHotspotsRequest,
    db: Session = DatabaseSession
):
    """Trouve les hotspots à proximité d'un point géographique donné"""
    try:
        logger.info("POST /hotspots/nearby requested",
                   lat=request.latitude,
                   lng=request.longitude,
                   radius=request.radius_miles)
        
        result = await hotspot_service.get_nearby_hotspots(request)
        
        response = NearbyHotspotsResponse(**result)
        
        logger.info("POST /hotspots/nearby completed successfully",
                   hotspots_found=response.total_found)
        
        return response
        
    except ValueError as e:
        logger.warning("Invalid request for POST /hotspots/nearby", exception=e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Requête invalide: {str(e)}"
        )
    except Exception as e:
        logger.error("POST /hotspots/nearby failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la recherche de hotspots à proximité"
        )


@router.get(
    "/{hotspot_id}/analysis",
    response_model=HotspotAnalysis,
    summary="Analyse d'un hotspot",
    description="Effectue une analyse approfondie d'un hotspot pour recommandations"
)
async def analyze_hotspot(
    hotspot_id: str = Path(..., description="Identifiant unique du hotspot"),
    db: Session = DatabaseSession
):
    """Effectue une analyse approfondie d'un hotspot pour générer des recommandations"""
    try:
        logger.info("GET /hotspots/{hotspot_id}/analysis requested", hotspot_id=hotspot_id)
        
        analysis = await hotspot_service.analyze_hotspot(hotspot_id)
        
        response = HotspotAnalysis(**analysis)
        
        logger.info("GET /hotspots/{hotspot_id}/analysis completed successfully",
                   hotspot_id=hotspot_id,
                   confidence_score=response.confidence_score)
        
        return response
        
    except ValueError as e:
        logger.warning("Invalid hotspot ID for analysis",
                      exception=e,
                      hotspot_id=hotspot_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Hotspot {hotspot_id} non trouvé pour analyse"
        )
    except Exception as e:
        logger.error("GET /hotspots/{hotspot_id}/analysis failed",
                    exception=e,
                    hotspot_id=hotspot_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de l'analyse du hotspot"
        )


@router.get(
    "/ranking/top-dangerous",
    response_model=list,
    summary="Top des zones les plus dangereuses",
    description="Récupère le classement des zones les plus dangereuses"
)
async def get_top_dangerous_hotspots(
    limit: int = Query(20, ge=1, le=100, description="Nombre de hotspots à retourner"),
    state: Optional[str] = Query(None, description="Code état pour filtrer"),
    db: Session = DatabaseSession
):
    """Récupère le classement des zones les plus dangereuses"""
    try:
        logger.info("GET /hotspots/ranking/top-dangerous requested",
                   limit=limit,
                   state=state)
        
        # Utilisation du service avec filtres pour les plus dangereux
        filters = HotspotFilters(
            state=state,
            min_danger_score=7.0  # Seuil pour les zones très dangereuses
        )
        
        result = await hotspot_service.get_hotspots(
            filters=filters,
            limit=limit
        )
        
        # Ajout du rang dans le classement
        top_dangerous = []
        for i, hotspot in enumerate(result['hotspots'], 1):
            hotspot_data = hotspot.copy()
            hotspot_data['rank'] = i
            top_dangerous.append(hotspot_data)
        
        logger.info("GET /hotspots/ranking/top-dangerous completed successfully",
                   hotspots_count=len(top_dangerous))
        
        return top_dangerous
        
    except Exception as e:
        logger.error("GET /hotspots/ranking/top-dangerous failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération du classement"
        )


@router.get(
    "/map/geojson",
    response_model=dict,
    summary="Données GeoJSON pour cartographie",
    description="Récupère les hotspots au format GeoJSON pour affichage sur carte"
)
async def get_hotspots_geojson(
    min_danger_score: Optional[float] = Query(5.0, ge=0.0, le=10.0, description="Score de danger minimum"),
    limit: int = Query(200, ge=1, le=1000, description="Nombre maximum de hotspots"),
    db: Session = DatabaseSession
):
    """Récupère les hotspots au format GeoJSON pour affichage cartographique"""
    try:
        logger.info("GET /hotspots/map/geojson requested",
                   min_danger_score=min_danger_score,
                   limit=limit)
        
        # Récupération des hotspots avec filtre
        filters = HotspotFilters(min_danger_score=min_danger_score)
        result = await hotspot_service.get_hotspots(filters=filters, limit=limit)
        hotspots = result['hotspots']
        
        # Construction du GeoJSON
        features = []
        for hotspot in hotspots:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [hotspot['longitude'], hotspot['latitude']]
                },
                "properties": {
                    "id": hotspot['id'],
                    "state": hotspot['state'],
                    "city": hotspot['city'],
                    "danger_score": hotspot['danger_score'],
                    "risk_level": hotspot['risk_level'],
                    "accident_count": hotspot['accident_count'],
                    "avg_severity": hotspot['avg_severity'],
                    "safety_score": hotspot['safety_score'],
                    "radius_miles": hotspot['radius_miles']
                }
            }
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "total_features": len(features),
                "min_danger_score_filter": min_danger_score,
                "generated_at": result['generated_at'].isoformat()
            }
        }
        
        logger.info("GET /hotspots/map/geojson completed successfully",
                   features_count=len(features))
        
        return geojson
        
    except Exception as e:
        logger.error("GET /hotspots/map/geojson failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la génération du GeoJSON"
        )