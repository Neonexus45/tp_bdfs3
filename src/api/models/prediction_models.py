"""
Modèles Pydantic pour les prédictions ML
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator


class PredictionRequest(BaseModel):
    """Modèle de requête pour la prédiction de sévérité"""
    # Localisation géographique (obligatoire)
    state: str = Field(..., description="Code état (ex: CA, TX)")
    start_lat: float = Field(..., ge=-90.0, le=90.0, description="Latitude de début")
    start_lng: float = Field(..., ge=-180.0, le=180.0, description="Longitude de début")
    
    # Conditions météorologiques
    weather_condition: Optional[str] = Field(None, description="Condition météorologique")
    temperature_fahrenheit: Optional[float] = Field(None, ge=-50.0, le=150.0, description="Température en Fahrenheit")
    humidity_percent: Optional[float] = Field(None, ge=0.0, le=100.0, description="Humidité en pourcentage")
    pressure_in: Optional[float] = Field(None, ge=20.0, le=35.0, description="Pression atmosphérique en pouces")
    visibility_miles: Optional[float] = Field(None, ge=0.0, le=50.0, description="Visibilité en miles")
    wind_speed_mph: Optional[float] = Field(None, ge=0.0, le=200.0, description="Vitesse du vent en mph")
    wind_chill_fahrenheit: Optional[float] = Field(None, ge=-100.0, le=150.0, description="Refroidissement éolien")
    precipitation_in: Optional[float] = Field(None, ge=0.0, le=20.0, description="Précipitations en pouces")
    
    # Informations temporelles
    accident_hour: Optional[int] = Field(None, ge=0, le=23, description="Heure de l'accident (0-23)")
    accident_day_of_week: Optional[int] = Field(None, ge=0, le=6, description="Jour de la semaine (0=Lundi)")
    accident_month: Optional[int] = Field(None, ge=1, le=12, description="Mois de l'accident")
    
    # Infrastructure et environnement
    distance_miles: Optional[float] = Field(None, ge=0.0, description="Distance affectée en miles")
    amenity: Optional[bool] = Field(None, description="Présence d'équipements à proximité")
    bump: Optional[bool] = Field(None, description="Présence de dos d'âne")
    crossing: Optional[bool] = Field(None, description="Présence de passage à niveau")
    give_way: Optional[bool] = Field(None, description="Présence de cédez-le-passage")
    junction: Optional[bool] = Field(None, description="Présence de jonction")
    no_exit: Optional[bool] = Field(None, description="Présence de voie sans issue")
    railway: Optional[bool] = Field(None, description="Présence de voie ferrée")
    roundabout: Optional[bool] = Field(None, description="Présence de rond-point")
    station: Optional[bool] = Field(None, description="Présence de station")
    stop: Optional[bool] = Field(None, description="Présence de panneau stop")
    traffic_calming: Optional[bool] = Field(None, description="Présence de ralentisseur")
    traffic_signal: Optional[bool] = Field(None, description="Présence de feu de circulation")
    turning_loop: Optional[bool] = Field(None, description="Présence de boucle de retournement")
    
    # Conditions de circulation
    sunrise_sunset: Optional[str] = Field(None, description="Période jour/nuit (Day/Night)")
    civil_twilight: Optional[str] = Field(None, description="Crépuscule civil (Day/Night)")
    nautical_twilight: Optional[str] = Field(None, description="Crépuscule nautique (Day/Night)")
    astronomical_twilight: Optional[str] = Field(None, description="Crépuscule astronomique (Day/Night)")
    
    # Métadonnées de la requête
    city: Optional[str] = Field(None, description="Ville")
    county: Optional[str] = Field(None, description="Comté")
    zipcode: Optional[str] = Field(None, description="Code postal")
    timezone: Optional[str] = Field(None, description="Fuseau horaire")
    
    @validator('state')
    def validate_state_code(cls, v):
        # Liste des codes d'états américains valides
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
        }
        if v.upper() not in valid_states:
            raise ValueError(f'Code état invalide. Doit être un des: {", ".join(sorted(valid_states))}')
        return v.upper()
    
    @validator('weather_condition')
    def validate_weather_condition(cls, v):
        if v:
            valid_conditions = [
                'Clear', 'Cloudy', 'Overcast', 'Partly Cloudy', 'Mostly Cloudy',
                'Rain', 'Light Rain', 'Heavy Rain', 'Drizzle', 'Thunderstorm',
                'Snow', 'Light Snow', 'Heavy Snow', 'Sleet', 'Hail',
                'Fog', 'Mist', 'Haze', 'Smoke', 'Dust', 'Sand', 'Squalls',
                'Tornado', 'Windy', 'Fair'
            ]
            if v not in valid_conditions:
                raise ValueError(f'Condition météorologique invalide. Doit être une des: {", ".join(valid_conditions)}')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "state": "CA",
                "start_lat": 34.0522,
                "start_lng": -118.2437,
                "weather_condition": "Clear",
                "temperature_fahrenheit": 72.0,
                "humidity_percent": 65.0,
                "pressure_in": 29.92,
                "visibility_miles": 10.0,
                "wind_speed_mph": 5.0,
                "accident_hour": 17,
                "accident_day_of_week": 1,
                "accident_month": 6,
                "distance_miles": 0.5,
                "traffic_signal": True,
                "junction": True,
                "sunrise_sunset": "Day",
                "city": "Los Angeles",
                "county": "Los Angeles County"
            }
        }


class PredictionResponse(BaseModel):
    """Modèle de réponse pour la prédiction de sévérité"""
    predicted_severity: int = Field(..., ge=1, le=4, description="Sévérité prédite (1-4)")
    predicted_severity_name: str = Field(..., description="Nom de la sévérité prédite")
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Score de confiance de la prédiction")
    probabilities: Dict[str, float] = Field(..., description="Probabilités pour chaque classe de sévérité")
    model_used: str = Field(..., description="Nom du modèle utilisé")
    model_version: str = Field(..., description="Version du modèle utilisé")
    prediction_timestamp: datetime = Field(..., description="Horodatage de la prédiction")
    processing_time_ms: float = Field(..., ge=0.0, description="Temps de traitement en millisecondes")
    
    # Informations contextuelles
    risk_factors: List[str] = Field(..., description="Facteurs de risque identifiés")
    safety_recommendations: List[str] = Field(..., description="Recommandations de sécurité")
    
    class Config:
        schema_extra = {
            "example": {
                "predicted_severity": 2,
                "predicted_severity_name": "Moderate",
                "confidence_score": 0.85,
                "probabilities": {
                    "1": 0.15,
                    "2": 0.65,
                    "3": 0.18,
                    "4": 0.02
                },
                "model_used": "random_forest_severity",
                "model_version": "v1.2.0",
                "prediction_timestamp": "2023-12-01T14:30:00Z",
                "processing_time_ms": 45.2,
                "risk_factors": [
                    "Rush hour traffic",
                    "High traffic density area",
                    "Weather conditions"
                ],
                "safety_recommendations": [
                    "Réduire la vitesse",
                    "Maintenir une distance de sécurité",
                    "Être vigilant aux intersections"
                ]
            }
        }


class BatchPredictionRequest(BaseModel):
    """Modèle de requête pour les prédictions en lot"""
    predictions: List[PredictionRequest] = Field(..., min_items=1, max_items=100, description="Liste des requêtes de prédiction")
    model_name: Optional[str] = Field("random_forest", description="Nom du modèle à utiliser")
    include_details: bool = Field(True, description="Inclure les détails dans la réponse")
    
    @validator('predictions')
    def validate_batch_size(cls, v):
        if len(v) > 100:
            raise ValueError('Maximum 100 prédictions par lot')
        return v


class BatchPredictionResponse(BaseModel):
    """Modèle de réponse pour les prédictions en lot"""
    predictions: List[PredictionResponse] = Field(..., description="Liste des prédictions")
    batch_summary: Dict[str, Any] = Field(..., description="Résumé du lot")
    total_processing_time_ms: float = Field(..., ge=0.0, description="Temps total de traitement")
    model_used: str = Field(..., description="Modèle utilisé")
    batch_timestamp: datetime = Field(..., description="Horodatage du lot")
    
    class Config:
        schema_extra = {
            "example": {
                "predictions": [
                    {
                        "predicted_severity": 2,
                        "predicted_severity_name": "Moderate",
                        "confidence_score": 0.85,
                        "probabilities": {"1": 0.15, "2": 0.65, "3": 0.18, "4": 0.02},
                        "model_used": "random_forest_severity",
                        "model_version": "v1.2.0",
                        "prediction_timestamp": "2023-12-01T14:30:00Z",
                        "processing_time_ms": 45.2,
                        "risk_factors": ["Rush hour traffic"],
                        "safety_recommendations": ["Réduire la vitesse"]
                    }
                ],
                "batch_summary": {
                    "total_predictions": 1,
                    "avg_confidence": 0.85,
                    "severity_distribution": {"1": 0, "2": 1, "3": 0, "4": 0}
                },
                "total_processing_time_ms": 125.5,
                "model_used": "random_forest_severity",
                "batch_timestamp": "2023-12-01T14:30:00Z"
            }
        }


class ModelInfo(BaseModel):
    """Informations sur un modèle ML disponible"""
    name: str = Field(..., description="Nom du modèle")
    version: str = Field(..., description="Version du modèle")
    description: str = Field(..., description="Description du modèle")
    accuracy: float = Field(..., ge=0.0, le=1.0, description="Précision du modèle")
    training_date: datetime = Field(..., description="Date d'entraînement")
    features_count: int = Field(..., ge=0, description="Nombre de features")
    is_active: bool = Field(..., description="Modèle actif")
    
    class Config:
        schema_extra = {
            "example": {
                "name": "random_forest_severity",
                "version": "v1.2.0",
                "description": "Modèle Random Forest pour prédiction de sévérité",
                "accuracy": 0.87,
                "training_date": "2023-11-15T09:00:00Z",
                "features_count": 47,
                "is_active": True
            }
        }


class ModelsListResponse(BaseModel):
    """Liste des modèles ML disponibles"""
    models: List[ModelInfo] = Field(..., description="Liste des modèles")
    active_model: str = Field(..., description="Modèle actuellement actif")
    total_models: int = Field(..., ge=0, description="Nombre total de modèles")
    
    class Config:
        schema_extra = {
            "example": {
                "models": [
                    {
                        "name": "random_forest_severity",
                        "version": "v1.2.0",
                        "description": "Modèle Random Forest pour prédiction de sévérité",
                        "accuracy": 0.87,
                        "training_date": "2023-11-15T09:00:00Z",
                        "features_count": 47,
                        "is_active": True
                    }
                ],
                "active_model": "random_forest_severity",
                "total_models": 1
            }
        }