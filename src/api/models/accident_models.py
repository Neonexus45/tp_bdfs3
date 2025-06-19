"""
Modèles Pydantic pour les accidents
"""

from datetime import date, datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator


class AccidentBase(BaseModel):
    """Modèle de base pour un accident"""
    id: str = Field(..., description="Identifiant unique de l'accident")
    state: str = Field(..., description="Code état (ex: CA, TX)")
    city: Optional[str] = Field(None, description="Ville de l'accident")
    severity: int = Field(..., ge=1, le=4, description="Niveau de sévérité (1-4)")
    accident_date: date = Field(..., description="Date de l'accident")
    accident_hour: int = Field(..., ge=0, le=23, description="Heure de l'accident (0-23)")
    weather_category: Optional[str] = Field(None, description="Catégorie météorologique")
    temperature_category: Optional[str] = Field(None, description="Catégorie de température")
    infrastructure_count: int = Field(..., ge=0, description="Nombre d'infrastructures à proximité")
    safety_score: float = Field(..., ge=0.0, le=10.0, description="Score de sécurité (0-10)")
    distance_miles: float = Field(..., ge=0.0, description="Distance en miles")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "A-123456",
                "state": "CA",
                "city": "Los Angeles",
                "severity": 2,
                "accident_date": "2023-01-15",
                "accident_hour": 14,
                "weather_category": "Clear",
                "temperature_category": "Moderate",
                "infrastructure_count": 3,
                "safety_score": 7.5,
                "distance_miles": 2.3
            }
        }


class AccidentResponse(AccidentBase):
    """Modèle de réponse pour un accident"""
    pass


class AccidentDetailResponse(AccidentBase):
    """Modèle de réponse détaillée pour un accident"""
    start_lat: Optional[float] = Field(None, description="Latitude de début")
    start_lng: Optional[float] = Field(None, description="Longitude de début")
    weather_condition: Optional[str] = Field(None, description="Condition météorologique détaillée")
    temperature_fahrenheit: Optional[float] = Field(None, description="Température en Fahrenheit")
    humidity_percent: Optional[float] = Field(None, description="Humidité en pourcentage")
    visibility_miles: Optional[float] = Field(None, description="Visibilité en miles")
    wind_speed_mph: Optional[float] = Field(None, description="Vitesse du vent en mph")
    created_at: Optional[datetime] = Field(None, description="Date de création de l'enregistrement")
    updated_at: Optional[datetime] = Field(None, description="Date de dernière mise à jour")


class PaginationInfo(BaseModel):
    """Informations de pagination"""
    page: int = Field(..., ge=1, description="Numéro de page actuelle")
    size: int = Field(..., ge=1, le=1000, description="Taille de la page")
    total_pages: int = Field(..., ge=0, description="Nombre total de pages")
    total_items: int = Field(..., ge=0, description="Nombre total d'éléments")
    has_next: bool = Field(..., description="Indique s'il y a une page suivante")
    has_previous: bool = Field(..., description="Indique s'il y a une page précédente")


class AccidentListResponse(BaseModel):
    """Modèle de réponse pour une liste d'accidents"""
    accidents: List[AccidentResponse] = Field(..., description="Liste des accidents")
    pagination: PaginationInfo = Field(..., description="Informations de pagination")
    filters_applied: Dict[str, Any] = Field(..., description="Filtres appliqués à la requête")
    
    class Config:
        schema_extra = {
            "example": {
                "accidents": [
                    {
                        "id": "A-123456",
                        "state": "CA",
                        "city": "Los Angeles",
                        "severity": 2,
                        "accident_date": "2023-01-15",
                        "accident_hour": 14,
                        "weather_category": "Clear",
                        "temperature_category": "Moderate",
                        "infrastructure_count": 3,
                        "safety_score": 7.5,
                        "distance_miles": 2.3
                    }
                ],
                "pagination": {
                    "page": 1,
                    "size": 20,
                    "total_pages": 50,
                    "total_items": 1000,
                    "has_next": True,
                    "has_previous": False
                },
                "filters_applied": {
                    "state": "CA",
                    "severity": 2
                }
            }
        }


class AccidentFilters(BaseModel):
    """Modèle pour les filtres d'accidents"""
    state: Optional[str] = Field(None, description="Code état")
    city: Optional[str] = Field(None, description="Ville")
    severity: Optional[int] = Field(None, ge=1, le=4, description="Niveau de sévérité")
    date_from: Optional[date] = Field(None, description="Date de début")
    date_to: Optional[date] = Field(None, description="Date de fin")
    weather_category: Optional[str] = Field(None, description="Catégorie météorologique")
    min_safety_score: Optional[float] = Field(None, ge=0.0, le=10.0, description="Score de sécurité minimum")
    max_safety_score: Optional[float] = Field(None, ge=0.0, le=10.0, description="Score de sécurité maximum")
    
    @validator('date_to')
    def validate_date_range(cls, v, values):
        if v and 'date_from' in values and values['date_from']:
            if v < values['date_from']:
                raise ValueError('date_to doit être postérieure à date_from')
        return v
    
    @validator('max_safety_score')
    def validate_safety_score_range(cls, v, values):
        if v and 'min_safety_score' in values and values['min_safety_score']:
            if v < values['min_safety_score']:
                raise ValueError('max_safety_score doit être supérieur à min_safety_score')
        return v


class AccidentSortOptions(BaseModel):
    """Options de tri pour les accidents"""
    sort_by: str = Field("accident_date", description="Colonne de tri")
    sort_order: str = Field("desc", pattern="^(asc|desc)$", description="Ordre de tri")
    
    @validator('sort_by')
    def validate_sort_column(cls, v):
        allowed_columns = [
            'accident_date', 'severity', 'safety_score', 'distance_miles',
            'infrastructure_count', 'accident_hour', 'state', 'city'
        ]
        if v not in allowed_columns:
            raise ValueError(f'sort_by doit être une des valeurs: {", ".join(allowed_columns)}')
        return v