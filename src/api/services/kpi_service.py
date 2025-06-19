"""
Service pour la gestion des KPIs
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, date
from sqlalchemy.orm import Session

from ...common.utils.database_connector import DatabaseConnector
from ...common.utils.logger import Logger
from ...common.config.config_manager import ConfigManager
from ..models.kpi_models import (
    KPIMetricType, KPISecurityResponse, KPITemporalResponse, 
    KPIInfrastructureResponse, KPIMLPerformanceResponse, KPIFilters
)


class KPIService:
    """Service pour la gestion des KPIs"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("kpi_service", self.config_manager)
        self.db_connector = DatabaseConnector(self.config_manager)
        
        # Tables de la couche Gold
        self.kpis_security_table = "kpis_security"
        self.kpis_temporal_table = "kpis_temporal"
        self.kpis_infrastructure_table = "kpis_infrastructure"
        self.kpis_ml_performance_table = "kpis_ml_performance"
        self.accidents_table = "accidents_summary"
        self.hotspots_table = "hotspots"
        
        # Configuration du cache
        self.cache_ttl = 300  # 5 minutes
    
    async def get_security_kpis(
        self,
        state: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Récupère les KPIs de sécurité
        
        Args:
            state: Code état (optionnel)
            limit: Nombre maximum de résultats
        
        Returns:
            list: Liste des KPIs de sécurité
        """
        try:
            self.logger.info("Fetching security KPIs", state=state, limit=limit)
            
            # Construction de la requête
            query = f"""
            SELECT 
                s.state,
                s.city,
                s.accident_rate_per_100k,
                s.danger_index,
                s.total_accidents,
                s.avg_severity,
                s.last_updated,
                h.hotspot_rank,
                -- Distribution des sévérités (calculée dynamiquement)
                SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END) as severity_1,
                SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END) as severity_2,
                SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END) as severity_3,
                SUM(CASE WHEN a.severity = 4 THEN 1 ELSE 0 END) as severity_4
            FROM {self.kpis_security_table} s
            LEFT JOIN {self.hotspots_table} h ON s.state = h.state AND s.city = h.city
            LEFT JOIN {self.accidents_table} a ON s.state = a.state AND s.city = a.city
            """
            
            params = {}
            where_conditions = []
            
            if state:
                where_conditions.append("s.state = :state")
                params['state'] = state
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            query += """
            GROUP BY s.state, s.city, s.accident_rate_per_100k, s.danger_index, 
                     s.total_accidents, s.avg_severity, s.last_updated, h.hotspot_rank
            ORDER BY s.danger_index DESC
            LIMIT :limit
            """
            params['limit'] = limit
            
            result = self.db_connector.execute_query(query, params)
            
            # Transformation des résultats
            kpis = []
            for row in result:
                kpi = {
                    "state": row['state'],
                    "city": row['city'],
                    "accident_rate_per_100k": row['accident_rate_per_100k'],
                    "danger_index": row['danger_index'],
                    "severity_distribution": {
                        "1": row['severity_1'] or 0,
                        "2": row['severity_2'] or 0,
                        "3": row['severity_3'] or 0,
                        "4": row['severity_4'] or 0
                    },
                    "hotspot_rank": row['hotspot_rank'] or 999,
                    "last_updated": row['last_updated'],
                    "total_accidents": row['total_accidents'],
                    "avg_severity": row['avg_severity']
                }
                kpis.append(kpi)
            
            self.logger.info("Security KPIs fetched successfully", count=len(kpis))
            return kpis
            
        except Exception as e:
            self.logger.error("Failed to fetch security KPIs", exception=e)
            raise
    
    async def get_temporal_kpis(
        self,
        period: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Récupère les KPIs temporels
        
        Args:
            period: Type de période (hourly, daily, monthly, yearly)
            state: Code état (optionnel)
            limit: Nombre maximum de résultats
        
        Returns:
            list: Liste des KPIs temporels
        """
        try:
            self.logger.info("Fetching temporal KPIs", period=period, state=state, limit=limit)
            
            # Si pas de table KPI temporelle, on calcule à partir des accidents
            query = f"""
            SELECT 
                state,
                CASE 
                    WHEN :period = 'hourly' THEN CAST(accident_hour AS CHAR)
                    WHEN :period = 'daily' THEN DAYNAME(accident_date)
                    WHEN :period = 'monthly' THEN DATE_FORMAT(accident_date, '%Y-%m')
                    WHEN :period = 'yearly' THEN CAST(YEAR(accident_date) AS CHAR)
                    ELSE DATE_FORMAT(accident_date, '%Y-%m')
                END as period_value,
                COUNT(*) as accident_count,
                AVG(severity) as severity_avg,
                -- Calcul de la tendance (simplifié)
                CASE 
                    WHEN COUNT(*) > AVG(COUNT(*)) OVER() THEN 'up'
                    WHEN COUNT(*) < AVG(COUNT(*)) OVER() THEN 'down'
                    ELSE 'stable'
                END as trend_direction,
                -- Facteur saisonnier (simplifié)
                CASE 
                    WHEN MONTH(accident_date) IN (12, 1, 2) THEN 1.2
                    WHEN MONTH(accident_date) IN (6, 7, 8) THEN 0.9
                    ELSE 1.0
                END as seasonal_factor,
                -- Heure de pic pour les données horaires
                CASE 
                    WHEN :period = 'hourly' THEN accident_hour
                    ELSE NULL
                END as peak_hour
            FROM {self.accidents_table}
            """
            
            params = {'period': period or 'monthly'}
            where_conditions = []
            
            if state:
                where_conditions.append("state = :state")
                params['state'] = state
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            query += """
            GROUP BY state, period_value, peak_hour
            ORDER BY accident_count DESC
            LIMIT :limit
            """
            params['limit'] = limit
            
            result = self.db_connector.execute_query(query, params)
            
            # Transformation des résultats
            kpis = []
            for row in result:
                kpi = {
                    "period_type": period or 'monthly',
                    "period_value": row['period_value'],
                    "state": row['state'],
                    "accident_count": row['accident_count'],
                    "severity_avg": round(row['severity_avg'], 2),
                    "trend_direction": row['trend_direction'],
                    "seasonal_factor": row['seasonal_factor'],
                    "comparison_previous": None,  # À calculer avec plus de données
                    "peak_hour": row['peak_hour']
                }
                kpis.append(kpi)
            
            self.logger.info("Temporal KPIs fetched successfully", count=len(kpis))
            return kpis
            
        except Exception as e:
            self.logger.error("Failed to fetch temporal KPIs", exception=e)
            raise
    
    async def get_infrastructure_kpis(
        self,
        state: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Récupère les KPIs d'infrastructure
        
        Args:
            state: Code état (optionnel)
            limit: Nombre maximum de résultats
        
        Returns:
            list: Liste des KPIs d'infrastructure
        """
        try:
            self.logger.info("Fetching infrastructure KPIs", state=state, limit=limit)
            
            # Calcul des KPIs d'infrastructure à partir des données d'accidents
            query = f"""
            SELECT 
                state,
                'Traffic_Signal' as infrastructure_type,
                -- Corrélation avec les accidents (approximation)
                CASE 
                    WHEN AVG(CASE WHEN traffic_signal = 1 THEN severity ELSE NULL END) < AVG(severity) THEN -0.3
                    ELSE 0.2
                END as accident_correlation,
                -- Score d'impact sur la sécurité
                CASE 
                    WHEN SUM(CASE WHEN traffic_signal = 1 THEN 1 ELSE 0 END) > 0 THEN 8.0
                    ELSE 5.0
                END as safety_impact_score,
                -- Densité d'infrastructure (approximation)
                (SUM(CASE WHEN traffic_signal = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as infrastructure_density,
                -- Potentiel de réduction d'accidents
                CASE 
                    WHEN SUM(CASE WHEN traffic_signal = 1 THEN 1 ELSE 0 END) > 0 THEN 25.0
                    ELSE 45.0
                END as accident_reduction_potential,
                -- Priorité d'investissement
                CASE 
                    WHEN AVG(severity) > 2.5 THEN 1
                    WHEN AVG(severity) > 2.0 THEN 2
                    ELSE 3
                END as investment_priority
            FROM {self.accidents_table}
            """
            
            params = {}
            where_conditions = []
            
            if state:
                where_conditions.append("state = :state")
                params['state'] = state
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            query += """
            GROUP BY state
            
            UNION ALL
            
            SELECT 
                state,
                'Junction' as infrastructure_type,
                CASE 
                    WHEN AVG(CASE WHEN junction = 1 THEN severity ELSE NULL END) > AVG(severity) THEN 0.4
                    ELSE -0.1
                END as accident_correlation,
                CASE 
                    WHEN SUM(CASE WHEN junction = 1 THEN 1 ELSE 0 END) > 0 THEN 6.5
                    ELSE 4.0
                END as safety_impact_score,
                (SUM(CASE WHEN junction = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as infrastructure_density,
                CASE 
                    WHEN SUM(CASE WHEN junction = 1 THEN 1 ELSE 0 END) > 0 THEN 35.0
                    ELSE 20.0
                END as accident_reduction_potential,
                CASE 
                    WHEN AVG(severity) > 2.5 THEN 2
                    WHEN AVG(severity) > 2.0 THEN 3
                    ELSE 4
                END as investment_priority
            FROM {self.accidents_table}
            """
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            query += """
            GROUP BY state
            ORDER BY safety_impact_score DESC
            LIMIT :limit
            """
            params['limit'] = limit
            
            result = self.db_connector.execute_query(query, params)
            
            # Transformation des résultats
            kpis = []
            for row in result:
                kpi = {
                    "state": row['state'],
                    "infrastructure_type": row['infrastructure_type'],
                    "accident_correlation": round(row['accident_correlation'], 2),
                    "safety_impact_score": round(row['safety_impact_score'], 1),
                    "infrastructure_density": round(row['infrastructure_density'], 1),
                    "accident_reduction_potential": round(row['accident_reduction_potential'], 1),
                    "investment_priority": row['investment_priority']
                }
                kpis.append(kpi)
            
            self.logger.info("Infrastructure KPIs fetched successfully", count=len(kpis))
            return kpis
            
        except Exception as e:
            self.logger.error("Failed to fetch infrastructure KPIs", exception=e)
            raise
    
    async def get_ml_performance_kpis(
        self,
        model_name: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Récupère les KPIs de performance ML
        
        Args:
            model_name: Nom du modèle (optionnel)
            limit: Nombre maximum de résultats
        
        Returns:
            list: Liste des KPIs de performance ML
        """
        try:
            self.logger.info("Fetching ML performance KPIs", model_name=model_name, limit=limit)
            
            # KPIs de performance ML simulés (dans une vraie implémentation, 
            # ces données viendraient de MLflow ou d'une table de métriques)
            ml_kpis = [
                {
                    "model_name": "random_forest_severity",
                    "model_version": "v1.2.0",
                    "accuracy": 0.87,
                    "precision": 0.85,
                    "recall": 0.82,
                    "f1_score": 0.83,
                    "prediction_count": 15420,
                    "avg_prediction_time_ms": 45.2,
                    "model_drift_score": 0.12,
                    "last_retrained": datetime(2023, 11, 15, 9, 0, 0)
                },
                {
                    "model_name": "gradient_boosting_severity",
                    "model_version": "v1.1.0",
                    "accuracy": 0.84,
                    "precision": 0.82,
                    "recall": 0.80,
                    "f1_score": 0.81,
                    "prediction_count": 8750,
                    "avg_prediction_time_ms": 62.8,
                    "model_drift_score": 0.08,
                    "last_retrained": datetime(2023, 10, 20, 14, 30, 0)
                },
                {
                    "model_name": "neural_network_severity",
                    "model_version": "v2.0.0",
                    "accuracy": 0.89,
                    "precision": 0.88,
                    "recall": 0.86,
                    "f1_score": 0.87,
                    "prediction_count": 12300,
                    "avg_prediction_time_ms": 78.5,
                    "model_drift_score": 0.15,
                    "last_retrained": datetime(2023, 12, 1, 10, 0, 0)
                }
            ]
            
            # Filtrage par nom de modèle si spécifié
            if model_name:
                ml_kpis = [kpi for kpi in ml_kpis if kpi['model_name'] == model_name]
            
            # Limitation du nombre de résultats
            ml_kpis = ml_kpis[:limit]
            
            self.logger.info("ML performance KPIs fetched successfully", count=len(ml_kpis))
            return ml_kpis
            
        except Exception as e:
            self.logger.error("Failed to fetch ML performance KPIs", exception=e)
            raise
    
    async def get_kpis_by_metric(
        self,
        metric_type: KPIMetricType,
        filters: KPIFilters = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Récupère les KPIs par type de métrique
        
        Args:
            metric_type: Type de métrique KPI
            filters: Filtres à appliquer
            limit: Nombre maximum de résultats
        
        Returns:
            dict: Données KPI avec résumé
        """
        try:
            self.logger.info("Fetching KPIs by metric", metric_type=metric_type, limit=limit)
            
            # Extraction des paramètres de filtres
            state = filters.state if filters else None
            period = filters.period if filters else None
            
            # Récupération des données selon le type de métrique
            if metric_type == KPIMetricType.security:
                data = await self.get_security_kpis(state=state, limit=limit)
                summary = self._calculate_security_summary(data)
            
            elif metric_type == KPIMetricType.temporal:
                data = await self.get_temporal_kpis(period=period, state=state, limit=limit)
                summary = self._calculate_temporal_summary(data)
            
            elif metric_type == KPIMetricType.infrastructure:
                data = await self.get_infrastructure_kpis(state=state, limit=limit)
                summary = self._calculate_infrastructure_summary(data)
            
            elif metric_type == KPIMetricType.ml_performance:
                data = await self.get_ml_performance_kpis(limit=limit)
                summary = self._calculate_ml_performance_summary(data)
            
            else:
                raise ValueError(f"Type de métrique non supporté: {metric_type}")
            
            response = {
                "metric_type": metric_type,
                "data": data,
                "summary": summary,
                "generated_at": datetime.now(),
                "filters_applied": filters.dict(exclude_none=True) if filters else {}
            }
            
            self.logger.info("KPIs fetched successfully", 
                           metric_type=metric_type, 
                           count=len(data))
            
            return response
            
        except Exception as e:
            self.logger.error("Failed to fetch KPIs by metric", exception=e, metric_type=metric_type)
            raise
    
    def _calculate_security_summary(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcule le résumé des KPIs de sécurité"""
        if not data:
            return {}
        
        total_accidents = sum(kpi['total_accidents'] for kpi in data)
        avg_danger_index = sum(kpi['danger_index'] for kpi in data) / len(data)
        most_dangerous = max(data, key=lambda x: x['danger_index'])
        
        return {
            "total_locations": len(data),
            "total_accidents": total_accidents,
            "avg_danger_index": round(avg_danger_index, 2),
            "most_dangerous_location": f"{most_dangerous['city']}, {most_dangerous['state']}",
            "highest_danger_score": most_dangerous['danger_index']
        }
    
    def _calculate_temporal_summary(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcule le résumé des KPIs temporels"""
        if not data:
            return {}
        
        total_accidents = sum(kpi['accident_count'] for kpi in data)
        avg_severity = sum(kpi['severity_avg'] for kpi in data) / len(data)
        peak_period = max(data, key=lambda x: x['accident_count'])
        
        return {
            "total_periods": len(data),
            "total_accidents": total_accidents,
            "avg_severity": round(avg_severity, 2),
            "peak_period": peak_period['period_value'],
            "peak_accidents": peak_period['accident_count']
        }
    
    def _calculate_infrastructure_summary(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcule le résumé des KPIs d'infrastructure"""
        if not data:
            return {}
        
        avg_safety_impact = sum(kpi['safety_impact_score'] for kpi in data) / len(data)
        best_infrastructure = max(data, key=lambda x: x['safety_impact_score'])
        high_priority = [kpi for kpi in data if kpi['investment_priority'] <= 2]
        
        return {
            "total_infrastructure_types": len(set(kpi['infrastructure_type'] for kpi in data)),
            "avg_safety_impact": round(avg_safety_impact, 2),
            "best_infrastructure_type": best_infrastructure['infrastructure_type'],
            "high_priority_investments": len(high_priority)
        }
    
    def _calculate_ml_performance_summary(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcule le résumé des KPIs de performance ML"""
        if not data:
            return {}
        
        avg_accuracy = sum(kpi['accuracy'] for kpi in data) / len(data)
        best_model = max(data, key=lambda x: x['accuracy'])
        total_predictions = sum(kpi['prediction_count'] for kpi in data)
        
        return {
            "total_models": len(data),
            "avg_accuracy": round(avg_accuracy, 3),
            "best_model": best_model['model_name'],
            "best_accuracy": best_model['accuracy'],
            "total_predictions": total_predictions
        }