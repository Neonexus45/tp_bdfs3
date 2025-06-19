"""
Service pour la gestion des hotspots (zones dangereuses)
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import math
from sqlalchemy.orm import Session

from ...common.utils.database_connector import DatabaseConnector
from ...common.utils.logger import Logger
from ...common.config.config_manager import ConfigManager
from ..models.hotspot_models import HotspotResponse, HotspotFilters, NearbyHotspotsRequest


class HotspotService:
    """Service pour la gestion des hotspots"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("hotspot_service", self.config_manager)
        self.db_connector = DatabaseConnector(self.config_manager)
        
        # Tables de la couche Gold
        self.hotspots_table = "hotspots"
        self.accidents_table = "accidents_summary"
        self.kpis_security_table = "kpis_security"
        
        # Configuration du service
        self.default_limit = 50
        self.max_limit = 500
        self.cache_ttl = 300  # 5 minutes
    
    async def get_hotspots(
        self,
        filters: HotspotFilters = None,
        limit: int = None
    ) -> Dict[str, Any]:
        """
        Récupère une liste de hotspots avec filtres
        
        Args:
            filters: Filtres à appliquer
            limit: Nombre maximum de résultats
        
        Returns:
            dict: Liste des hotspots avec métadonnées
        """
        try:
            if limit is None:
                limit = self.default_limit
            limit = min(limit, self.max_limit)
            
            self.logger.info("Fetching hotspots", 
                           limit=limit, 
                           filters=filters.dict() if filters else None)
            
            # Construction de la requête de base
            query = f"""
            SELECT 
                h.id,
                h.state,
                h.city,
                h.county,
                h.latitude,
                h.longitude,
                h.danger_score,
                h.accident_count,
                h.radius_miles,
                h.avg_severity,
                h.accident_rate_per_day,
                h.safety_score,
                h.risk_level,
                h.last_updated,
                -- Calcul des heures de pic (approximation)
                GROUP_CONCAT(DISTINCT 
                    CASE 
                        WHEN a.accident_hour BETWEEN 7 AND 9 THEN a.accident_hour
                        WHEN a.accident_hour BETWEEN 17 AND 19 THEN a.accident_hour
                        ELSE NULL
                    END
                ) as peak_hours_str,
                -- Distribution des sévérités
                SUM(CASE WHEN a.severity = 1 THEN 1 ELSE 0 END) as severity_1,
                SUM(CASE WHEN a.severity = 2 THEN 1 ELSE 0 END) as severity_2,
                SUM(CASE WHEN a.severity = 3 THEN 1 ELSE 0 END) as severity_3,
                SUM(CASE WHEN a.severity = 4 THEN 1 ELSE 0 END) as severity_4
            FROM {self.hotspots_table} h
            LEFT JOIN {self.accidents_table} a ON h.state = a.state AND h.city = a.city
            """
            
            # Construction des filtres
            where_conditions = []
            params = {}
            
            if filters:
                filter_dict = filters.dict(exclude_none=True)
                
                if filter_dict.get('state'):
                    where_conditions.append("h.state = :state")
                    params['state'] = filter_dict['state']
                
                if filter_dict.get('city'):
                    where_conditions.append("h.city LIKE :city")
                    params['city'] = f"%{filter_dict['city']}%"
                
                if filter_dict.get('min_danger_score') is not None:
                    where_conditions.append("h.danger_score >= :min_danger_score")
                    params['min_danger_score'] = filter_dict['min_danger_score']
                
                if filter_dict.get('max_danger_score') is not None:
                    where_conditions.append("h.danger_score <= :max_danger_score")
                    params['max_danger_score'] = filter_dict['max_danger_score']
                
                if filter_dict.get('risk_level'):
                    where_conditions.append("h.risk_level = :risk_level")
                    params['risk_level'] = filter_dict['risk_level']
                
                if filter_dict.get('min_accident_count') is not None:
                    where_conditions.append("h.accident_count >= :min_accident_count")
                    params['min_accident_count'] = filter_dict['min_accident_count']
                
                # Filtrage géographique par rayon
                if (filter_dict.get('center_lat') is not None and 
                    filter_dict.get('center_lng') is not None and 
                    filter_dict.get('search_radius_miles') is not None):
                    
                    # Utilisation de la formule de distance haversine approximée
                    where_conditions.append("""
                        (6371 * acos(cos(radians(:center_lat)) * cos(radians(h.latitude)) * 
                         cos(radians(h.longitude) - radians(:center_lng)) + 
                         sin(radians(:center_lat)) * sin(radians(h.latitude)))) * 0.621371 <= :search_radius_miles
                    """)
                    params['center_lat'] = filter_dict['center_lat']
                    params['center_lng'] = filter_dict['center_lng']
                    params['search_radius_miles'] = filter_dict['search_radius_miles']
            
            # Ajout de la clause WHERE
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Groupement et tri
            query += """
            GROUP BY h.id, h.state, h.city, h.county, h.latitude, h.longitude, 
                     h.danger_score, h.accident_count, h.radius_miles, h.avg_severity,
                     h.accident_rate_per_day, h.safety_score, h.risk_level, h.last_updated
            ORDER BY h.danger_score DESC
            LIMIT :limit
            """
            params['limit'] = limit
            
            # Exécution de la requête
            result = self.db_connector.execute_query(query, params)
            
            # Transformation des résultats
            hotspots = []
            for row in result:
                hotspot = self._transform_hotspot_row(row)
                hotspots.append(hotspot)
            
            # Calcul du résumé
            summary = await self._calculate_hotspots_summary(hotspots, filters)
            
            response = {
                "hotspots": hotspots,
                "total_hotspots": len(hotspots),
                "filters_applied": filters.dict(exclude_none=True) if filters else {},
                "summary": summary,
                "generated_at": datetime.now()
            }
            
            self.logger.info("Hotspots fetched successfully", count=len(hotspots))
            return response
            
        except Exception as e:
            self.logger.error("Failed to fetch hotspots", exception=e)
            raise
    
    async def get_hotspot_by_id(self, hotspot_id: str) -> Optional[Dict[str, Any]]:
        """
        Récupère un hotspot par son ID avec détails complets
        
        Args:
            hotspot_id: ID du hotspot
        
        Returns:
            dict: Données détaillées du hotspot ou None
        """
        try:
            self.logger.info("Fetching hotspot by ID", hotspot_id=hotspot_id)
            
            query = f"""
            SELECT 
                h.*,
                -- Statistiques détaillées des accidents
                COUNT(a.id) as total_accidents_detailed,
                AVG(a.severity) as avg_severity_detailed,
                -- Impact météorologique
                AVG(CASE WHEN a.weather_condition = 'Clear' THEN a.severity ELSE NULL END) as clear_weather_severity,
                AVG(CASE WHEN a.weather_condition LIKE '%Rain%' THEN a.severity ELSE NULL END) as rain_weather_severity,
                AVG(CASE WHEN a.weather_condition LIKE '%Fog%' THEN a.severity ELSE NULL END) as fog_weather_severity,
                -- Patterns temporels
                AVG(CASE WHEN a.accident_hour BETWEEN 7 AND 9 OR a.accident_hour BETWEEN 17 AND 19 THEN a.severity ELSE NULL END) as rush_hour_severity,
                AVG(CASE WHEN DAYOFWEEK(a.accident_date) IN (1, 7) THEN a.severity ELSE NULL END) as weekend_severity
            FROM {self.hotspots_table} h
            LEFT JOIN {self.accidents_table} a ON h.state = a.state AND h.city = a.city
            WHERE h.id = :hotspot_id
            GROUP BY h.id
            """
            
            result = self.db_connector.execute_query(query, {"hotspot_id": hotspot_id})
            
            if not result:
                self.logger.warning("Hotspot not found", hotspot_id=hotspot_id)
                return None
            
            hotspot_data = result[0]
            
            # Transformation en hotspot détaillé
            hotspot = self._transform_hotspot_detail_row(hotspot_data)
            
            self.logger.info("Hotspot fetched successfully", hotspot_id=hotspot_id)
            return hotspot
            
        except Exception as e:
            self.logger.error("Failed to fetch hotspot by ID", exception=e, hotspot_id=hotspot_id)
            raise
    
    async def get_nearby_hotspots(
        self,
        request: NearbyHotspotsRequest
    ) -> Dict[str, Any]:
        """
        Trouve les hotspots à proximité d'un point donné
        
        Args:
            request: Requête de recherche de proximité
        
        Returns:
            dict: Hotspots à proximité avec distances
        """
        try:
            self.logger.info("Finding nearby hotspots", 
                           lat=request.latitude, 
                           lng=request.longitude, 
                           radius=request.radius_miles)
            
            query = f"""
            SELECT 
                h.*,
                (6371 * acos(cos(radians(:lat)) * cos(radians(h.latitude)) * 
                 cos(radians(h.longitude) - radians(:lng)) + 
                 sin(radians(:lat)) * sin(radians(h.latitude)))) * 0.621371 as distance_miles
            FROM {self.hotspots_table} h
            WHERE (6371 * acos(cos(radians(:lat)) * cos(radians(h.latitude)) * 
                   cos(radians(h.longitude) - radians(:lng)) + 
                   sin(radians(:lat)) * sin(radians(h.latitude)))) * 0.621371 <= :radius
            """
            
            params = {
                'lat': request.latitude,
                'lng': request.longitude,
                'radius': request.radius_miles
            }
            
            # Filtre par score de danger minimum
            if request.min_danger_score is not None:
                query += " AND h.danger_score >= :min_danger_score"
                params['min_danger_score'] = request.min_danger_score
            
            query += " ORDER BY distance_miles ASC LIMIT :limit"
            params['limit'] = request.limit
            
            result = self.db_connector.execute_query(query, params)
            
            # Transformation des résultats
            hotspots = []
            distances = {}
            
            for row in result:
                hotspot = self._transform_hotspot_row(row)
                hotspots.append(hotspot)
                distances[hotspot['id']] = round(row['distance_miles'], 2)
            
            response = {
                "reference_point": {
                    "latitude": request.latitude,
                    "longitude": request.longitude
                },
                "search_radius_miles": request.radius_miles,
                "hotspots": hotspots,
                "distances": distances,
                "total_found": len(hotspots)
            }
            
            self.logger.info("Nearby hotspots found", count=len(hotspots))
            return response
            
        except Exception as e:
            self.logger.error("Failed to find nearby hotspots", exception=e)
            raise
    
    async def analyze_hotspot(self, hotspot_id: str) -> Dict[str, Any]:
        """
        Effectue une analyse approfondie d'un hotspot
        
        Args:
            hotspot_id: ID du hotspot à analyser
        
        Returns:
            dict: Résultats de l'analyse
        """
        try:
            self.logger.info("Analyzing hotspot", hotspot_id=hotspot_id)
            
            # Récupération des données détaillées du hotspot
            hotspot = await self.get_hotspot_by_id(hotspot_id)
            if not hotspot:
                raise ValueError(f"Hotspot {hotspot_id} non trouvé")
            
            # Analyse des améliorations potentielles
            current_safety_score = hotspot['safety_score']
            danger_score = hotspot['danger_score']
            
            # Calcul du potentiel d'amélioration
            potential_improvement = min(10.0 - current_safety_score, danger_score * 0.4)
            estimated_accident_reduction = min(50.0, danger_score * 5.0)
            
            # Calcul du ROI approximatif (en mois)
            investment_cost = danger_score * 10000  # Coût approximatif
            annual_savings = estimated_accident_reduction * 1000  # Économies approximatives
            roi_months = (investment_cost / annual_savings * 12) if annual_savings > 0 else 999
            
            # Recommandations basées sur l'analyse
            recommendations = self._generate_safety_recommendations(hotspot)
            
            analysis = {
                "hotspot_id": hotspot_id,
                "analysis_type": "safety_improvement",
                "results": {
                    "current_safety_score": current_safety_score,
                    "potential_improvement": round(potential_improvement, 1),
                    "estimated_accident_reduction": round(estimated_accident_reduction, 1),
                    "roi_months": min(int(roi_months), 999)
                },
                "recommendations": recommendations,
                "confidence_score": 0.75,  # Score de confiance approximatif
                "analysis_date": datetime.now()
            }
            
            self.logger.info("Hotspot analysis completed", hotspot_id=hotspot_id)
            return analysis
            
        except Exception as e:
            self.logger.error("Failed to analyze hotspot", exception=e, hotspot_id=hotspot_id)
            raise
    
    def _transform_hotspot_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme une ligne de résultat en objet hotspot
        
        Args:
            row: Ligne de résultat de la base de données
        
        Returns:
            dict: Hotspot transformé
        """
        # Parsing des heures de pic
        peak_hours_str = row.get('peak_hours_str', '')
        peak_hours = []
        if peak_hours_str:
            try:
                peak_hours = [int(h) for h in peak_hours_str.split(',') if h.strip().isdigit()]
            except:
                peak_hours = [7, 8, 17, 18, 19]  # Valeurs par défaut
        
        # Distribution des sévérités
        severity_distribution = {
            "1": row.get('severity_1', 0) or 0,
            "2": row.get('severity_2', 0) or 0,
            "3": row.get('severity_3', 0) or 0,
            "4": row.get('severity_4', 0) or 0
        }
        
        # Principales causes (simulées basées sur les données)
        main_causes = []
        if row.get('avg_severity', 0) > 2.5:
            main_causes.append("High severity accidents")
        if row.get('accident_rate_per_day', 0) > 1.0:
            main_causes.append("High traffic density")
        main_causes.append("Weather conditions")
        
        # Infrastructures à proximité (simulées)
        infrastructure_nearby = ["Traffic Signal", "Junction"]
        if row.get('danger_score', 0) > 7.0:
            infrastructure_nearby.append("Bus Station")
        
        return {
            "id": row.get('id'),
            "state": row.get('state'),
            "city": row.get('city'),
            "county": row.get('county'),
            "latitude": row.get('latitude'),
            "longitude": row.get('longitude'),
            "danger_score": row.get('danger_score', 0.0),
            "accident_count": row.get('accident_count', 0),
            "radius_miles": row.get('radius_miles', 0.5),
            "severity_distribution": severity_distribution,
            "avg_severity": row.get('avg_severity', 0.0),
            "accident_rate_per_day": row.get('accident_rate_per_day', 0.0),
            "peak_hours": peak_hours,
            "main_causes": main_causes,
            "infrastructure_nearby": infrastructure_nearby,
            "safety_score": row.get('safety_score', 0.0),
            "risk_level": row.get('risk_level', 'Medium'),
            "last_updated": row.get('last_updated', datetime.now())
        }
    
    def _transform_hotspot_detail_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme une ligne de résultat en objet hotspot détaillé
        
        Args:
            row: Ligne de résultat de la base de données
        
        Returns:
            dict: Hotspot détaillé transformé
        """
        base_hotspot = self._transform_hotspot_row(row)
        
        # Ajout des détails supplémentaires
        weather_impact = {
            "Clear": 0.3,
            "Rain": row.get('rain_weather_severity', 2.0) / row.get('avg_severity_detailed', 2.0) if row.get('avg_severity_detailed') else 0.8,
            "Fog": row.get('fog_weather_severity', 2.5) / row.get('avg_severity_detailed', 2.0) if row.get('avg_severity_detailed') else 0.9
        }
        
        temporal_patterns = {
            "rush_hour_factor": row.get('rush_hour_severity', 2.0) / row.get('avg_severity_detailed', 2.0) if row.get('avg_severity_detailed') else 1.5,
            "weekend_factor": row.get('weekend_severity', 1.8) / row.get('avg_severity_detailed', 2.0) if row.get('avg_severity_detailed') else 0.8,
            "seasonal_peak": "Winter"
        }
        
        accident_trends = {
            "trend_direction": "stable",
            "monthly_change": 0.0,
            "yearly_change": 0.0
        }
        
        safety_recommendations = self._generate_safety_recommendations(base_hotspot)
        
        # Priorité d'investissement basée sur le score de danger
        danger_score = base_hotspot['danger_score']
        if danger_score >= 8.0:
            investment_priority = 1
        elif danger_score >= 6.0:
            investment_priority = 2
        elif danger_score >= 4.0:
            investment_priority = 3
        else:
            investment_priority = 4
        
        estimated_cost_reduction = danger_score * 15000.0  # Estimation approximative
        
        base_hotspot.update({
            "weather_impact": weather_impact,
            "temporal_patterns": temporal_patterns,
            "accident_trends": accident_trends,
            "safety_recommendations": safety_recommendations,
            "investment_priority": investment_priority,
            "estimated_cost_reduction": estimated_cost_reduction
        })
        
        return base_hotspot
    
    def _generate_safety_recommendations(self, hotspot: Dict[str, Any]) -> List[str]:
        """
        Génère des recommandations de sécurité pour un hotspot
        
        Args:
            hotspot: Données du hotspot
        
        Returns:
            list: Liste des recommandations
        """
        recommendations = []
        danger_score = hotspot.get('danger_score', 0)
        avg_severity = hotspot.get('avg_severity', 0)
        
        if danger_score >= 8.0:
            recommendations.append("Installer des feux de circulation intelligents")
            recommendations.append("Renforcer l'éclairage public")
            recommendations.append("Ajouter des barrières de sécurité")
        
        if danger_score >= 6.0:
            recommendations.append("Améliorer la signalisation routière")
            recommendations.append("Installer des ralentisseurs")
        
        if avg_severity >= 3.0:
            recommendations.append("Réduire la limite de vitesse")
            recommendations.append("Installer des caméras de surveillance")
        
        if "Junction" in hotspot.get('infrastructure_nearby', []):
            recommendations.append("Optimiser la synchronisation des feux")
        
        recommendations.append("Sensibiliser les conducteurs locaux")
        
        return recommendations[:5]  # Limiter à 5 recommandations
    
    async def _calculate_hotspots_summary(
        self, 
        hotspots: List[Dict[str, Any]], 
        filters: HotspotFilters = None
    ) -> Dict[str, Any]:
        """
        Calcule le résumé des hotspots
        
        Args:
            hotspots: Liste des hotspots
            filters: Filtres appliqués
        
        Returns:
            dict: Résumé des hotspots
        """
        if not hotspots:
            return {}
        
        total_accidents = sum(h['accident_count'] for h in hotspots)
        avg_danger_score = sum(h['danger_score'] for h in hotspots) / len(hotspots)
        
        # Hotspot le plus dangereux
        most_dangerous = max(hotspots, key=lambda x: x['danger_score'])
        most_dangerous_city = f"{most_dangerous['city']}, {most_dangerous['state']}"
        
        # Comptage des hotspots critiques
        critical_hotspots = len([h for h in hotspots if h['danger_score'] >= 8.0])
        
        return {
            "avg_danger_score": round(avg_danger_score, 2),
            "total_accidents": total_accidents,
            "most_dangerous_city": most_dangerous_city,
            "critical_hotspots": critical_hotspots,
            "risk_level_distribution": {
                "Critical": len([h for h in hotspots if h['risk_level'] == 'Critical']),
                "High": len([h for h in hotspots if h['risk_level'] == 'High']),
                "Medium": len([h for h in hotspots if h['risk_level'] == 'Medium']),
                "Low": len([h for h in hotspots if h['risk_level'] == 'Low'])
            }
        }
    
    def calculate_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """
        Calcule la distance entre deux points géographiques (formule haversine)
        
        Args:
            lat1, lng1: Coordonnées du premier point
            lat2, lng2: Coordonnées du second point
        
        Returns:
            float: Distance en miles
        """
        # Conversion en radians
        lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
        
        # Formule haversine
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Rayon de la Terre en miles
        r = 3956
        
        return c * r