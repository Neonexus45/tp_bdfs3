"""
Modèles Pydantic pour les hotspots (zones dangereuses)
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator


class HotspotBase(BaseModel):
    """Modèle de base pour un hotspot"""
    id: str = Field(..., description="Identifiant unique du hotspot")
    state: str = Field(..., description="Code état")
    city: Optional[str] = Field(None, description="Ville")
    county: Optional[str] = Field(None, description="Comté")
    latitude: float = Field(..., ge=-90.0, le=90.0, description="Latitude du centre")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="Longitude du centre")
    danger_score: float = Field(..., ge=0.0, le=10.0, description="Score de danger (0-10)")
    accident_count: int = Field(..., ge=0, description="Nombre d'accidents dans la zone")
    radius_miles: float = Field(..., ge=0.0, description="Rayon de la zone en miles")


class HotspotResponse(HotspotBase):
    """Modèle de réponse pour un hotspot"""
    severity_distribution: Dict[str, int] = Field(..., description="Distribution des sévérités")
    avg_severity: float = Field(..., ge=1.0, le=4.0, description="Sévérité moyenne")
    accident_rate_per_day: float = Field(..., ge=0.0, description="Taux d'accidents par jour")
    peak_hours: List[int] = Field(..., description="Heures de pic d'accidents")
    main_causes: List[str] = Field(..., description="Principales causes d'accidents")
    infrastructure_nearby: List[str] = Field(..., description="Infrastructures à proximité")
    safety_score: float = Field(..., ge=0.0, le=10.0, description="Score de sécurité")
    risk_level: str = Field(..., description="Niveau de risque (Low, Medium, High, Critical)")
    last_updated: datetime = Field(..., description="Dernière mise à jour")
    
    @validator('risk_level')
    def validate_risk_level(cls, v):
        valid_levels = ['Low', 'Medium', 'High', 'Critical']
        if v not in valid_levels:
            raise ValueError(f'Niveau de risque invalide. Doit être un des: {", ".join(valid_levels)}')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "id": "HS-CA-LA-001",
                "state": "CA",
                "city": "Los Angeles",
                "county": "Los Angeles County",
                "latitude": 34.0522,
                "longitude": -118.2437,
                "danger_score": 8.5,
                "accident_count": 245,
                "radius_miles": 0.5,
                "severity_distribution": {
                    "1": 98,
                    "2": 89,
                    "3": 45,
                    "4": 13
                },
                "avg_severity": 2.1,
                "accident_rate_per_day": 0.67,
                "peak_hours": [7, 8, 17, 18, 19],
                "main_causes": [
                    "Intersection collision",
                    "Weather conditions",
                    "High traffic density"
                ],
                "infrastructure_nearby": [
                    "Traffic Signal",
                    "Junction",
                    "Bus Station"
                ],
                "safety_score": 3.2,
                "risk_level": "High",
                "last_updated": "2023-12-01T10:30:00Z"
            }
        }


class HotspotDetailResponse(HotspotResponse):
    """Modèle de réponse détaillée pour un hotspot"""
    weather_impact: Dict[str, float] = Field(..., description="Impact des conditions météorologiques")
    temporal_patterns: Dict[str, Any] = Field(..., description="Patterns temporels")
    accident_trends: Dict[str, Any] = Field(..., description="Tendances des accidents")
    safety_recommendations: List[str] = Field(..., description="Recommandations de sécurité")
    investment_priority: int = Field(..., ge=1, le=5, description="Priorité d'investissement (1-5)")
    estimated_cost_reduction: float = Field(..., ge=0.0, description="Réduction estimée des coûts ($)")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "HS-CA-LA-001",
                "state": "CA",
                "city": "Los Angeles",
                "latitude": 34.0522,
                "longitude": -118.2437,
                "danger_score": 8.5,
                "accident_count": 245,
                "radius_miles": 0.5,
                "severity_distribution": {"1": 98, "2": 89, "3": 45, "4": 13},
                "avg_severity": 2.1,
                "accident_rate_per_day": 0.67,
                "peak_hours": [7, 8, 17, 18, 19],
                "main_causes": ["Intersection collision", "Weather conditions"],
                "infrastructure_nearby": ["Traffic Signal", "Junction"],
                "safety_score": 3.2,
                "risk_level": "High",
                "last_updated": "2023-12-01T10:30:00Z",
                "weather_impact": {
                    "Clear": 0.3,
                    "Rain": 0.8,
                    "Fog": 0.9
                },
                "temporal_patterns": {
                    "rush_hour_factor": 2.5,
                    "weekend_factor": 0.7,
                    "seasonal_peak": "Winter"
                },
                "accident_trends": {
                    "trend_direction": "increasing",
                    "monthly_change": 5.2,
                    "yearly_change": -2.1
                },
                "safety_recommendations": [
                    "Installer des feux de circulation intelligents",
                    "Améliorer l'éclairage",
                    "Ajouter des panneaux de signalisation"
                ],
                "investment_priority": 2,
                "estimated_cost_reduction": 125000.0
            }
        }


class HotspotListResponse(BaseModel):
    """Modèle de réponse pour une liste de hotspots"""
    hotspots: List[HotspotResponse] = Field(..., description="Liste des hotspots")
    total_hotspots: int = Field(..., ge=0, description="Nombre total de hotspots")
    filters_applied: Dict[str, Any] = Field(..., description="Filtres appliqués")
    summary: Dict[str, Any] = Field(..., description="Résumé des hotspots")
    generated_at: datetime = Field(..., description="Date de génération")
    
    class Config:
        schema_extra = {
            "example": {
                "hotspots": [
                    {
                        "id": "HS-CA-LA-001",
                        "state": "CA",
                        "city": "Los Angeles",
                        "latitude": 34.0522,
                        "longitude": -118.2437,
                        "danger_score": 8.5,
                        "accident_count": 245,
                        "risk_level": "High"
                    }
                ],
                "total_hotspots": 150,
                "filters_applied": {
                    "state": "CA",
                    "min_danger_score": 7.0
                },
                "summary": {
                    "avg_danger_score": 6.8,
                    "total_accidents": 15420,
                    "most_dangerous_city": "Los Angeles",
                    "critical_hotspots": 12
                },
                "generated_at": "2023-12-01T10:30:00Z"
            }
        }


class HotspotFilters(BaseModel):
    """Modèle pour les filtres de hotspots"""
    state: Optional[str] = Field(None, description="Code état")
    city: Optional[str] = Field(None, description="Ville")
    min_danger_score: Optional[float] = Field(None, ge=0.0, le=10.0, description="Score de danger minimum")
    max_danger_score: Optional[float] = Field(None, ge=0.0, le=10.0, description="Score de danger maximum")
    risk_level: Optional[str] = Field(None, description="Niveau de risque")
    min_accident_count: Optional[int] = Field(None, ge=0, description="Nombre minimum d'accidents")
    center_lat: Optional[float] = Field(None, ge=-90.0, le=90.0, description="Latitude centre recherche")
    center_lng: Optional[float] = Field(None, ge=-180.0, le=180.0, description="Longitude centre recherche")
    search_radius_miles: Optional[float] = Field(None, ge=0.0, description="Rayon de recherche en miles")
    
    @validator('max_danger_score')
    def validate_danger_score_range(cls, v, values):
        if v and 'min_danger_score' in values and values['min_danger_score']:
            if v < values['min_danger_score']:
                raise ValueError('max_danger_score doit être supérieur à min_danger_score')
        return v
    
    @validator('risk_level')
    def validate_risk_level(cls, v):
        if v:
            valid_levels = ['Low', 'Medium', 'High', 'Critical']
            if v not in valid_levels:
                raise ValueError(f'Niveau de risque invalide. Doit être un des: {", ".join(valid_levels)}')
        return v


class HotspotAnalysis(BaseModel):
    """Modèle pour l'analyse d'un hotspot"""
    hotspot_id: str = Field(..., description="ID du hotspot analysé")
    analysis_type: str = Field(..., description="Type d'analyse")
    results: Dict[str, Any] = Field(..., description="Résultats de l'analyse")
    recommendations: List[str] = Field(..., description="Recommandations")
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Score de confiance de l'analyse")
    analysis_date: datetime = Field(..., description="Date de l'analyse")
    
    class Config:
        schema_extra = {
            "example": {
                "hotspot_id": "HS-CA-LA-001",
                "analysis_type": "safety_improvement",
                "results": {
                    "current_safety_score": 3.2,
                    "potential_improvement": 2.8,
                    "estimated_accident_reduction": 35.0,
                    "roi_months": 18
                },
                "recommendations": [
                    "Installer des feux intelligents",
                    "Améliorer la signalisation",
                    "Renforcer l'éclairage"
                ],
                "confidence_score": 0.87,
                "analysis_date": "2023-12-01T10:30:00Z"
            }
        }


class NearbyHotspotsRequest(BaseModel):
    """Requête pour trouver les hotspots à proximité"""
    latitude: float = Field(..., ge=-90.0, le=90.0, description="Latitude du point de référence")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="Longitude du point de référence")
    radius_miles: float = Field(5.0, ge=0.1, le=50.0, description="Rayon de recherche en miles")
    min_danger_score: Optional[float] = Field(None, ge=0.0, le=10.0, description="Score de danger minimum")
    limit: int = Field(10, ge=1, le=100, description="Nombre maximum de résultats")
    
    class Config:
        schema_extra = {
            "example": {
                "latitude": 34.0522,
                "longitude": -118.2437,
                "radius_miles": 10.0,
                "min_danger_score": 5.0,
                "limit": 20
            }
        }


class NearbyHotspotsResponse(BaseModel):
    """Réponse pour les hotspots à proximité"""
    reference_point: Dict[str, float] = Field(..., description="Point de référence")
    search_radius_miles: float = Field(..., description="Rayon de recherche utilisé")
    hotspots: List[HotspotResponse] = Field(..., description="Hotspots trouvés")
    distances: Dict[str, float] = Field(..., description="Distances depuis le point de référence")
    total_found: int = Field(..., description="Nombre total trouvé")
    
    class Config:
        schema_extra = {
            "example": {
                "reference_point": {
                    "latitude": 34.0522,
                    "longitude": -118.2437
                },
                "search_radius_miles": 10.0,
                "hotspots": [
                    {
                        "id": "HS-CA-LA-001",
                        "state": "CA",
                        "city": "Los Angeles",
                        "latitude": 34.0522,
                        "longitude": -118.2437,
                        "danger_score": 8.5,
                        "risk_level": "High"
                    }
                ],
                "distances": {
                    "HS-CA-LA-001": 2.3
                },
                "total_found": 1
            }
        }