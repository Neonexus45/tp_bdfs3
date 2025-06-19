"""
Router pour les endpoints de prédiction ML (version simplifiée)
"""

from typing import Optional, List
from fastapi import APIRouter, HTTPException, status, Query, Body
from sqlalchemy.orm import Session

from ..models.prediction_models import (
    PredictionRequest, PredictionResponse, BatchPredictionRequest, 
    ModelsListResponse
)
from ..models.response_models import ErrorResponse
from ..services.prediction_service import APIPredictionService
from ..dependencies.database import DatabaseSession
from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger

# Configuration du router
router = APIRouter(
    prefix="/predict",
    tags=["Prédictions ML"],
    responses={
        400: {"model": ErrorResponse, "description": "Données d'entrée invalides"},
        500: {"model": ErrorResponse, "description": "Erreur interne du serveur"}
    }
)

# Instances globales
config_manager = ConfigManager()
logger = Logger("predictions_router", config_manager)
prediction_service = APIPredictionService(config_manager)


@router.post(
    "",
    response_model=PredictionResponse,
    summary="Prédiction de sévérité",
    description="Prédit la sévérité d'un accident en temps réel"
)
async def predict_severity(
    request: PredictionRequest = Body(..., description="Données de l'accident à analyser"),
    model_name: str = Query("random_forest", description="Nom du modèle ML à utiliser"),
    db: Session = DatabaseSession
):
    """Prédit la sévérité d'un accident en temps réel basé sur les conditions données"""
    try:
        logger.info("POST /predict requested",
                   state=request.state,
                   model_name=model_name)
        
        # Initialisation du service ML si nécessaire
        await prediction_service.initialize_ml_service()
        
        # Prédiction
        prediction = await prediction_service.predict_severity(
            request=request,
            model_name=model_name
        )
        
        logger.info("POST /predict completed successfully",
                   predicted_severity=prediction.predicted_severity,
                   confidence=prediction.confidence_score,
                   processing_time=prediction.processing_time_ms)
        
        return prediction
        
    except ValueError as e:
        logger.warning("Invalid prediction request", exception=e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Données d'entrée invalides: {str(e)}"
        )
    except Exception as e:
        logger.error("POST /predict failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la prédiction"
        )


@router.post(
    "/batch",
    response_model=dict,
    summary="Prédictions en lot",
    description="Effectue des prédictions sur un lot d'accidents"
)
async def predict_batch(
    request: BatchPredictionRequest = Body(..., description="Lot de prédictions à effectuer"),
    db: Session = DatabaseSession
):
    """Effectue des prédictions de sévérité sur un lot d'accidents"""
    try:
        logger.info("POST /predict/batch requested",
                   batch_size=len(request.predictions),
                   model_name=request.model_name)
        
        # Validation de la taille du lot
        if len(request.predictions) > 100:
            raise ValueError("Maximum 100 prédictions par lot")
        
        # Initialisation du service ML si nécessaire
        await prediction_service.initialize_ml_service()
        
        # Prédictions en lot
        result = await prediction_service.predict_batch(request)
        
        logger.info("POST /predict/batch completed successfully",
                   total_predictions=result['batch_summary']['total_predictions'],
                   failed_predictions=result.get('failed_predictions', 0),
                   total_time=result['total_processing_time_ms'])
        
        return result
        
    except ValueError as e:
        logger.warning("Invalid batch prediction request", exception=e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Requête de lot invalide: {str(e)}"
        )
    except Exception as e:
        logger.error("POST /predict/batch failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors des prédictions en lot"
        )


@router.get(
    "/models",
    response_model=ModelsListResponse,
    summary="Modèles disponibles",
    description="Récupère la liste des modèles ML disponibles"
)
async def get_available_models(
    db: Session = DatabaseSession
):
    """Récupère la liste des modèles de machine learning disponibles pour les prédictions"""
    try:
        logger.info("GET /predict/models requested")
        
        models = await prediction_service.get_available_models()
        
        # Construction de la réponse
        active_model = next((m['name'] for m in models if m['is_active']), models[0]['name'] if models else None)
        
        response = ModelsListResponse(
            models=models,
            active_model=active_model,
            total_models=len(models)
        )
        
        logger.info("GET /predict/models completed successfully",
                   models_count=len(models),
                   active_model=active_model)
        
        return response
        
    except Exception as e:
        logger.error("GET /predict/models failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des modèles"
        )


@router.get(
    "/health",
    response_model=dict,
    summary="Santé du service de prédiction",
    description="Vérifie la santé du service de prédiction ML"
)
async def get_prediction_service_health(
    db: Session = DatabaseSession
):
    """Vérifie la santé du service de prédiction ML"""
    try:
        logger.info("GET /predict/health requested")
        
        health_status = await prediction_service.health_check()
        
        logger.info("GET /predict/health completed successfully",
                   status=health_status['status'])
        
        return health_status
        
    except Exception as e:
        logger.error("GET /predict/health failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la vérification de santé"
        )


@router.get(
    "/metrics",
    response_model=dict,
    summary="Métriques du service",
    description="Récupère les métriques détaillées du service de prédiction"
)
async def get_prediction_metrics(
    db: Session = DatabaseSession
):
    """Récupère les métriques détaillées du service de prédiction"""
    try:
        logger.info("GET /predict/metrics requested")
        
        metrics = prediction_service.get_service_metrics()
        
        # Ajout de métriques calculées
        total_predictions = metrics['total_predictions']
        if total_predictions > 0:
            success_rate = (metrics['successful_predictions'] / total_predictions) * 100
            error_rate = (metrics['failed_predictions'] / total_predictions) * 100
        else:
            success_rate = 0.0
            error_rate = 0.0
        
        cache_total = metrics['cache_hits'] + metrics['cache_misses']
        cache_hit_rate = (metrics['cache_hits'] / cache_total * 100) if cache_total > 0 else 0.0
        
        enhanced_metrics = {
            **metrics,
            'success_rate_percent': round(success_rate, 2),
            'error_rate_percent': round(error_rate, 2),
            'cache_hit_rate_percent': round(cache_hit_rate, 2),
            'service_uptime_info': {
                'status': 'running',
                'initialized': prediction_service.ml_prediction_service is not None
            }
        }
        
        logger.info("GET /predict/metrics completed successfully",
                   total_predictions=total_predictions,
                   success_rate=success_rate)
        
        return enhanced_metrics
        
    except Exception as e:
        logger.error("GET /predict/metrics failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des métriques"
        )


@router.post(
    "/validate",
    response_model=dict,
    summary="Validation des données",
    description="Valide les données d'entrée sans effectuer de prédiction"
)
async def validate_prediction_data(
    request: PredictionRequest = Body(..., description="Données à valider"),
    db: Session = DatabaseSession
):
    """Valide les données d'entrée pour une prédiction sans l'effectuer"""
    try:
        logger.info("POST /predict/validate requested", state=request.state)
        
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'suggestions': [],
            'data_quality_score': 0.0,
            'missing_optional_fields': [],
            'completeness_percentage': 0.0
        }
        
        # Validation des champs requis
        required_fields = ['state', 'start_lat', 'start_lng']
        missing_required = []
        
        for field in required_fields:
            if not getattr(request, field, None):
                missing_required.append(field)
        
        if missing_required:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"Champs requis manquants: {', '.join(missing_required)}")
        
        # Validation des coordonnées
        if request.start_lat and (request.start_lat < -90 or request.start_lat > 90):
            validation_result['errors'].append("Latitude invalide (doit être entre -90 et 90)")
            validation_result['is_valid'] = False
        
        if request.start_lng and (request.start_lng < -180 or request.start_lng > 180):
            validation_result['errors'].append("Longitude invalide (doit être entre -180 et 180)")
            validation_result['is_valid'] = False
        
        # Validation du code état
        valid_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC']
        if request.state and request.state.upper() not in valid_states:
            validation_result['errors'].append(f"Code état invalide: {request.state}")
            validation_result['is_valid'] = False
        
        # Vérification des champs optionnels pour suggestions
        optional_important_fields = [
            'weather_condition', 'temperature_fahrenheit', 'humidity_percent',
            'visibility_miles', 'accident_hour', 'accident_day_of_week'
        ]
        
        missing_optional = []
        total_optional = len(optional_important_fields)
        provided_optional = 0
        
        for field in optional_important_fields:
            value = getattr(request, field, None)
            if value is None:
                missing_optional.append(field)
            else:
                provided_optional += 1
        
        validation_result['missing_optional_fields'] = missing_optional
        validation_result['completeness_percentage'] = round((provided_optional / total_optional) * 100, 1)
        
        # Calcul du score de qualité
        base_score = 0.6 if validation_result['is_valid'] else 0.0
        completeness_bonus = (provided_optional / total_optional) * 0.4
        validation_result['data_quality_score'] = round(base_score + completeness_bonus, 2)
        
        # Suggestions d'amélioration
        if missing_optional:
            validation_result['suggestions'].append(
                f"Ajouter ces données optionnelles améliorerait la précision: {', '.join(missing_optional[:3])}"
            )
        
        if validation_result['completeness_percentage'] < 50:
            validation_result['warnings'].append("Données incomplètes - la précision de la prédiction pourrait être réduite")
        
        logger.info("POST /predict/validate completed successfully",
                   is_valid=validation_result['is_valid'],
                   quality_score=validation_result['data_quality_score'])
        
        return validation_result
        
    except Exception as e:
        logger.error("POST /predict/validate failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la validation"
        )


@router.get(
    "/examples",
    response_model=List[dict],
    summary="Exemples de données",
    description="Récupère des exemples de données pour les prédictions"
)
async def get_prediction_examples(
    db: Session = DatabaseSession
):
    """Récupère des exemples de données pour faciliter l'utilisation de l'API de prédiction"""
    try:
        logger.info("GET /predict/examples requested")
        
        examples = [
            {
                "name": "Accident urbain - Los Angeles, CA",
                "description": "Accident typique en zone urbaine dense",
                "data": {
                    "state": "CA",
                    "start_lat": 34.0522,
                    "start_lng": -118.2437,
                    "weather_condition": "Clear",
                    "temperature_fahrenheit": 72.0,
                    "humidity_percent": 65.0,
                    "visibility_miles": 10.0,
                    "accident_hour": 17,
                    "accident_day_of_week": 1,
                    "traffic_signal": True,
                    "junction": True,
                    "city": "Los Angeles"
                }
            },
            {
                "name": "Accident rural - Texas",
                "description": "Accident sur route rurale avec peu d'infrastructure",
                "data": {
                    "state": "TX",
                    "start_lat": 31.7619,
                    "start_lng": -106.4850,
                    "weather_condition": "Clear",
                    "temperature_fahrenheit": 85.0,
                    "humidity_percent": 45.0,
                    "visibility_miles": 15.0,
                    "accident_hour": 14,
                    "accident_day_of_week": 3,
                    "traffic_signal": False,
                    "junction": False
                }
            },
            {
                "name": "Accident par mauvais temps - Floride",
                "description": "Accident pendant une tempête",
                "data": {
                    "state": "FL",
                    "start_lat": 25.7617,
                    "start_lng": -80.1918,
                    "weather_condition": "Heavy Rain",
                    "temperature_fahrenheit": 78.0,
                    "humidity_percent": 85.0,
                    "visibility_miles": 2.0,
                    "wind_speed_mph": 25.0,
                    "precipitation_in": 0.5,
                    "accident_hour": 20,
                    "accident_day_of_week": 5
                }
            }
        ]
        
        logger.info("GET /predict/examples completed successfully", examples_count=len(examples))
        
        return examples
        
    except Exception as e:
        logger.error("GET /predict/examples failed", exception=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des exemples"
        )