"""
Modèles Pydantic pour les KPIs
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator
from enum import Enum


class KPIMetricType(str, Enum):
    """Types de métriques KPI disponibles"""
    security = "security"
    temporal = "temporal"
    infrastructure = "infrastructure"
    ml_performance = "ml_performance"


class TrendDirection(str, Enum):
    """Direction de tendance"""
    up = "up"
    down = "down"
    stable = "stable"


class PeriodType(str, Enum):
    """Types de période pour les KPIs temporels"""
    hourly = "hourly"
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"
    yearly = "yearly"


class KPISecurityResponse(BaseModel):
    """Modèle de réponse pour les KPIs de sécurité"""
    state: str = Field(..., description="Code état")
    city: Optional[str] = Field(None, description="Ville (optionnel)")
    accident_rate_per_100k: float = Field(..., ge=0.0, description="Taux d'accidents pour 100k habitants")
    danger_index: float = Field(..., ge=0.0, le=10.0, description="Index de danger (0-10)")
    severity_distribution: Dict[str, int] = Field(..., description="Distribution des sévérités")
    hotspot_rank: int = Field(..., ge=1, description="Rang dans les zones dangereuses")
    last_updated: datetime = Field(..., description="Dernière mise à jour")
    total_accidents: int = Field(..., ge=0, description="Nombre total d'accidents")
    avg_severity: float = Field(..., ge=1.0, le=4.0, description="Sévérité moyenne")
    
    class Config:
        schema_extra = {
            "example": {
                "state": "CA",
                "city": "Los Angeles",
                "accident_rate_per_100k": 125.5,
                "danger_index": 7.2,
                "severity_distribution": {
                    "1": 450,
                    "2": 320,
                    "3": 180,
                    "4": 50
                },
                "hotspot_rank": 3,
                "last_updated": "2023-12-01T10:30:00Z",
                "total_accidents": 1000,
                "avg_severity": 2.1
            }
        }


class KPITemporalResponse(BaseModel):
    """Modèle de réponse pour les KPIs temporels"""
    period_type: PeriodType = Field(..., description="Type de période")
    period_value: str = Field(..., description="Valeur de la période (ex: '2023-01', 'Monday')")
    state: str = Field(..., description="Code état")
    accident_count: int = Field(..., ge=0, description="Nombre d'accidents")
    severity_avg: float = Field(..., ge=1.0, le=4.0, description="Sévérité moyenne")
    trend_direction: TrendDirection = Field(..., description="Direction de la tendance")
    seasonal_factor: float = Field(..., description="Facteur saisonnier")
    comparison_previous: Optional[float] = Field(None, description="Comparaison avec période précédente (%)")
    peak_hour: Optional[int] = Field(None, ge=0, le=23, description="Heure de pic (si applicable)")
    
    class Config:
        schema_extra = {
            "example": {
                "period_type": "monthly",
                "period_value": "2023-01",
                "state": "CA",
                "accident_count": 1250,
                "severity_avg": 2.3,
                "trend_direction": "up",
                "seasonal_factor": 1.15,
                "comparison_previous": 8.5,
                "peak_hour": 17
            }
        }


class KPIInfrastructureResponse(BaseModel):
    """Modèle de réponse pour les KPIs d'infrastructure"""
    state: str = Field(..., description="Code état")
    infrastructure_type: str = Field(..., description="Type d'infrastructure")
    accident_correlation: float = Field(..., ge=-1.0, le=1.0, description="Corrélation avec les accidents")
    safety_impact_score: float = Field(..., ge=0.0, le=10.0, description="Score d'impact sur la sécurité")
    infrastructure_density: float = Field(..., ge=0.0, description="Densité d'infrastructure par km²")
    accident_reduction_potential: float = Field(..., ge=0.0, le=100.0, description="Potentiel de réduction d'accidents (%)")
    investment_priority: int = Field(..., ge=1, le=5, description="Priorité d'investissement (1-5)")
    
    class Config:
        schema_extra = {
            "example": {
                "state": "CA",
                "infrastructure_type": "Traffic_Signal",
                "accident_correlation": -0.65,
                "safety_impact_score": 8.2,
                "infrastructure_density": 12.5,
                "accident_reduction_potential": 35.0,
                "investment_priority": 2
            }
        }


class KPIMLPerformanceResponse(BaseModel):
    """Modèle de réponse pour les KPIs de performance ML"""
    model_name: str = Field(..., description="Nom du modèle")
    model_version: str = Field(..., description="Version du modèle")
    accuracy: float = Field(..., ge=0.0, le=1.0, description="Précision du modèle")
    precision: float = Field(..., ge=0.0, le=1.0, description="Précision par classe")
    recall: float = Field(..., ge=0.0, le=1.0, description="Rappel")
    f1_score: float = Field(..., ge=0.0, le=1.0, description="Score F1")
    prediction_count: int = Field(..., ge=0, description="Nombre de prédictions effectuées")
    avg_prediction_time_ms: float = Field(..., ge=0.0, description="Temps moyen de prédiction (ms)")
    model_drift_score: float = Field(..., ge=0.0, le=1.0, description="Score de dérive du modèle")
    last_retrained: Optional[datetime] = Field(None, description="Dernière date de réentraînement")
    
    class Config:
        schema_extra = {
            "example": {
                "model_name": "random_forest_severity",
                "model_version": "v1.2.0",
                "accuracy": 0.87,
                "precision": 0.85,
                "recall": 0.82,
                "f1_score": 0.83,
                "prediction_count": 15420,
                "avg_prediction_time_ms": 45.2,
                "model_drift_score": 0.12,
                "last_retrained": "2023-11-15T09:00:00Z"
            }
        }


class KPIListResponse(BaseModel):
    """Modèle de réponse générique pour une liste de KPIs"""
    metric_type: KPIMetricType = Field(..., description="Type de métrique")
    data: List[Dict[str, Any]] = Field(..., description="Données des KPIs")
    summary: Dict[str, Any] = Field(..., description="Résumé des métriques")
    generated_at: datetime = Field(..., description="Date de génération")
    filters_applied: Dict[str, Any] = Field(..., description="Filtres appliqués")
    
    class Config:
        schema_extra = {
            "example": {
                "metric_type": "security",
                "data": [
                    {
                        "state": "CA",
                        "accident_rate_per_100k": 125.5,
                        "danger_index": 7.2
                    }
                ],
                "summary": {
                    "total_states": 50,
                    "avg_accident_rate": 98.3,
                    "highest_danger_state": "CA"
                },
                "generated_at": "2023-12-01T10:30:00Z",
                "filters_applied": {
                    "state": "CA"
                }
            }
        }


class KPIFilters(BaseModel):
    """Modèle pour les filtres de KPIs"""
    state: Optional[str] = Field(None, description="Code état")
    period: Optional[str] = Field(None, description="Période (format dépend du type)")
    date_from: Optional[datetime] = Field(None, description="Date de début")
    date_to: Optional[datetime] = Field(None, description="Date de fin")
    min_value: Optional[float] = Field(None, description="Valeur minimum")
    max_value: Optional[float] = Field(None, description="Valeur maximum")
    
    @validator('date_to')
    def validate_date_range(cls, v, values):
        if v and 'date_from' in values and values['date_from']:
            if v < values['date_from']:
                raise ValueError('date_to doit être postérieure à date_from')
        return v


class KPISummaryResponse(BaseModel):
    """Modèle de réponse pour un résumé de KPIs"""
    total_accidents: int = Field(..., ge=0, description="Nombre total d'accidents")
    avg_severity: float = Field(..., ge=1.0, le=4.0, description="Sévérité moyenne")
    most_dangerous_state: str = Field(..., description="État le plus dangereux")
    safest_state: str = Field(..., description="État le plus sûr")
    trend_overall: TrendDirection = Field(..., description="Tendance générale")
    last_updated: datetime = Field(..., description="Dernière mise à jour")
    data_quality_score: float = Field(..., ge=0.0, le=1.0, description="Score de qualité des données")
    
    class Config:
        schema_extra = {
            "example": {
                "total_accidents": 125000,
                "avg_severity": 2.1,
                "most_dangerous_state": "CA",
                "safest_state": "VT",
                "trend_overall": "stable",
                "last_updated": "2023-12-01T10:30:00Z",
                "data_quality_score": 0.95
            }
        }