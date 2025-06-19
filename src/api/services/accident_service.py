"""
Service pour la gestion des accidents
"""

from typing import List, Dict, Any, Optional
from datetime import date
from sqlalchemy.orm import Session

from ...common.utils.database_connector import DatabaseConnector
from ...common.utils.logger import Logger
from ...common.config.config_manager import ConfigManager
from ..models.accident_models import AccidentResponse, AccidentFilters, AccidentSortOptions
from ..dependencies.database import execute_query_with_pagination, build_where_clause, build_order_clause


class AccidentService:
    """Service pour la gestion des données d'accidents"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("accident_service", self.config_manager)
        self.db_connector = DatabaseConnector(self.config_manager)
        
        # Configuration du service
        self.default_page_size = self.config_manager.get('api.default_page_size', 20)
        self.max_page_size = self.config_manager.get('api.max_page_size', 1000)
        
        # Tables de la couche Gold
        self.accidents_table = "accidents_summary"
        self.cache_ttl = 300  # 5 minutes
    
    async def get_accidents(
        self,
        page: int = 1,
        size: int = None,
        filters: AccidentFilters = None,
        sort_options: AccidentSortOptions = None
    ) -> Dict[str, Any]:
        """
        Récupère une liste paginée d'accidents avec filtres et tri
        
        Args:
            page: Numéro de page
            size: Taille de la page
            filters: Filtres à appliquer
            sort_options: Options de tri
        
        Returns:
            dict: Résultats paginés avec métadonnées
        """
        try:
            # Validation des paramètres
            if size is None:
                size = self.default_page_size
            size = min(size, self.max_page_size)
            
            self.logger.info("Fetching accidents", 
                           page=page, 
                           size=size, 
                           filters=filters.dict() if filters else None)
            
            # Construction de la requête de base
            base_query = f"""
            SELECT 
                id,
                state,
                city,
                severity,
                accident_date,
                accident_hour,
                weather_category,
                temperature_category,
                infrastructure_count,
                safety_score,
                distance_miles,
                start_lat,
                start_lng,
                weather_condition,
                temperature_fahrenheit,
                humidity_percent,
                visibility_miles,
                wind_speed_mph,
                created_at,
                updated_at
            FROM {self.accidents_table}
            """
            
            # Construction des filtres
            where_conditions = []
            params = {}
            
            if filters:
                filter_dict = filters.dict(exclude_none=True)
                
                # Filtres spécifiques
                if filter_dict.get('state'):
                    where_conditions.append("state = :state")
                    params['state'] = filter_dict['state']
                
                if filter_dict.get('city'):
                    where_conditions.append("city LIKE :city")
                    params['city'] = f"%{filter_dict['city']}%"
                
                if filter_dict.get('severity'):
                    where_conditions.append("severity = :severity")
                    params['severity'] = filter_dict['severity']
                
                if filter_dict.get('date_from'):
                    where_conditions.append("accident_date >= :date_from")
                    params['date_from'] = filter_dict['date_from']
                
                if filter_dict.get('date_to'):
                    where_conditions.append("accident_date <= :date_to")
                    params['date_to'] = filter_dict['date_to']
                
                if filter_dict.get('weather_category'):
                    where_conditions.append("weather_category = :weather_category")
                    params['weather_category'] = filter_dict['weather_category']
                
                if filter_dict.get('min_safety_score') is not None:
                    where_conditions.append("safety_score >= :min_safety_score")
                    params['min_safety_score'] = filter_dict['min_safety_score']
                
                if filter_dict.get('max_safety_score') is not None:
                    where_conditions.append("safety_score <= :max_safety_score")
                    params['max_safety_score'] = filter_dict['max_safety_score']
            
            # Ajout de la clause WHERE
            if where_conditions:
                base_query += " WHERE " + " AND ".join(where_conditions)
            
            # Ajout du tri
            if sort_options:
                order_clause = build_order_clause(sort_options.sort_by, sort_options.sort_order)
                if order_clause:
                    base_query += " " + order_clause
            else:
                base_query += " ORDER BY accident_date DESC"
            
            # Exécution de la requête paginée
            result = execute_query_with_pagination(
                query=base_query,
                params=params,
                page=page,
                size=size,
                db_connector=self.db_connector
            )
            
            # Transformation des résultats
            accidents = [self._transform_accident_row(row) for row in result['data']]
            
            response = {
                "accidents": accidents,
                "pagination": result['pagination'],
                "filters_applied": filters.dict(exclude_none=True) if filters else {},
                "sort_applied": sort_options.dict() if sort_options else {"sort_by": "accident_date", "sort_order": "desc"}
            }
            
            self.logger.info("Accidents fetched successfully", 
                           count=len(accidents),
                           total_items=result['pagination']['total_items'])
            
            return response
            
        except Exception as e:
            self.logger.error("Failed to fetch accidents", exception=e)
            raise
    
    async def get_accident_by_id(self, accident_id: str) -> Optional[Dict[str, Any]]:
        """
        Récupère un accident par son ID
        
        Args:
            accident_id: ID de l'accident
        
        Returns:
            dict: Données de l'accident ou None
        """
        try:
            self.logger.info("Fetching accident by ID", accident_id=accident_id)
            
            query = f"""
            SELECT 
                id,
                state,
                city,
                county,
                severity,
                accident_date,
                accident_hour,
                weather_category,
                temperature_category,
                infrastructure_count,
                safety_score,
                distance_miles,
                start_lat,
                start_lng,
                end_lat,
                end_lng,
                weather_condition,
                temperature_fahrenheit,
                humidity_percent,
                pressure_in,
                visibility_miles,
                wind_speed_mph,
                wind_chill_fahrenheit,
                precipitation_in,
                amenity,
                bump,
                crossing,
                give_way,
                junction,
                no_exit,
                railway,
                roundabout,
                station,
                stop,
                traffic_calming,
                traffic_signal,
                turning_loop,
                sunrise_sunset,
                civil_twilight,
                nautical_twilight,
                astronomical_twilight,
                created_at,
                updated_at
            FROM {self.accidents_table}
            WHERE id = :accident_id
            """
            
            result = self.db_connector.execute_query(query, {"accident_id": accident_id})
            
            if not result:
                self.logger.warning("Accident not found", accident_id=accident_id)
                return None
            
            accident = self._transform_accident_detail_row(result[0])
            
            self.logger.info("Accident fetched successfully", accident_id=accident_id)
            return accident
            
        except Exception as e:
            self.logger.error("Failed to fetch accident by ID", exception=e, accident_id=accident_id)
            raise
    
    async def get_accidents_summary(self, filters: AccidentFilters = None) -> Dict[str, Any]:
        """
        Récupère un résumé des accidents
        
        Args:
            filters: Filtres à appliquer
        
        Returns:
            dict: Résumé des accidents
        """
        try:
            self.logger.info("Fetching accidents summary")
            
            # Construction des filtres
            where_conditions = []
            params = {}
            
            if filters:
                filter_dict = filters.dict(exclude_none=True)
                
                if filter_dict.get('state'):
                    where_conditions.append("state = :state")
                    params['state'] = filter_dict['state']
                
                if filter_dict.get('date_from'):
                    where_conditions.append("accident_date >= :date_from")
                    params['date_from'] = filter_dict['date_from']
                
                if filter_dict.get('date_to'):
                    where_conditions.append("accident_date <= :date_to")
                    params['date_to'] = filter_dict['date_to']
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Requête de résumé
            summary_query = f"""
            SELECT 
                COUNT(*) as total_accidents,
                AVG(severity) as avg_severity,
                MIN(accident_date) as earliest_date,
                MAX(accident_date) as latest_date,
                COUNT(DISTINCT state) as states_count,
                COUNT(DISTINCT city) as cities_count,
                SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) as severity_1_count,
                SUM(CASE WHEN severity = 2 THEN 1 ELSE 0 END) as severity_2_count,
                SUM(CASE WHEN severity = 3 THEN 1 ELSE 0 END) as severity_3_count,
                SUM(CASE WHEN severity = 4 THEN 1 ELSE 0 END) as severity_4_count,
                AVG(safety_score) as avg_safety_score,
                AVG(distance_miles) as avg_distance_miles
            FROM {self.accidents_table}
            {where_clause}
            """
            
            result = self.db_connector.execute_query(summary_query, params)
            
            if result:
                summary_data = result[0]
                summary = {
                    "total_accidents": summary_data['total_accidents'],
                    "avg_severity": round(summary_data['avg_severity'], 2) if summary_data['avg_severity'] else 0,
                    "date_range": {
                        "earliest": summary_data['earliest_date'],
                        "latest": summary_data['latest_date']
                    },
                    "geographic_coverage": {
                        "states_count": summary_data['states_count'],
                        "cities_count": summary_data['cities_count']
                    },
                    "severity_distribution": {
                        "1": summary_data['severity_1_count'],
                        "2": summary_data['severity_2_count'],
                        "3": summary_data['severity_3_count'],
                        "4": summary_data['severity_4_count']
                    },
                    "avg_safety_score": round(summary_data['avg_safety_score'], 2) if summary_data['avg_safety_score'] else 0,
                    "avg_distance_miles": round(summary_data['avg_distance_miles'], 2) if summary_data['avg_distance_miles'] else 0,
                    "filters_applied": filters.dict(exclude_none=True) if filters else {}
                }
            else:
                summary = {
                    "total_accidents": 0,
                    "message": "Aucune donnée trouvée"
                }
            
            self.logger.info("Accidents summary fetched successfully", 
                           total_accidents=summary.get('total_accidents', 0))
            
            return summary
            
        except Exception as e:
            self.logger.error("Failed to fetch accidents summary", exception=e)
            raise
    
    def _transform_accident_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme une ligne de résultat en objet accident
        
        Args:
            row: Ligne de résultat de la base de données
        
        Returns:
            dict: Accident transformé
        """
        return {
            "id": row.get('id'),
            "state": row.get('state'),
            "city": row.get('city'),
            "severity": row.get('severity'),
            "accident_date": row.get('accident_date'),
            "accident_hour": row.get('accident_hour'),
            "weather_category": row.get('weather_category'),
            "temperature_category": row.get('temperature_category'),
            "infrastructure_count": row.get('infrastructure_count', 0),
            "safety_score": row.get('safety_score', 0.0),
            "distance_miles": row.get('distance_miles', 0.0)
        }
    
    def _transform_accident_detail_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme une ligne de résultat en objet accident détaillé
        
        Args:
            row: Ligne de résultat de la base de données
        
        Returns:
            dict: Accident détaillé transformé
        """
        base_accident = self._transform_accident_row(row)
        
        # Ajout des détails supplémentaires
        base_accident.update({
            "start_lat": row.get('start_lat'),
            "start_lng": row.get('start_lng'),
            "end_lat": row.get('end_lat'),
            "end_lng": row.get('end_lng'),
            "county": row.get('county'),
            "weather_condition": row.get('weather_condition'),
            "temperature_fahrenheit": row.get('temperature_fahrenheit'),
            "humidity_percent": row.get('humidity_percent'),
            "pressure_in": row.get('pressure_in'),
            "visibility_miles": row.get('visibility_miles'),
            "wind_speed_mph": row.get('wind_speed_mph'),
            "wind_chill_fahrenheit": row.get('wind_chill_fahrenheit'),
            "precipitation_in": row.get('precipitation_in'),
            "infrastructure": {
                "amenity": row.get('amenity', False),
                "bump": row.get('bump', False),
                "crossing": row.get('crossing', False),
                "give_way": row.get('give_way', False),
                "junction": row.get('junction', False),
                "no_exit": row.get('no_exit', False),
                "railway": row.get('railway', False),
                "roundabout": row.get('roundabout', False),
                "station": row.get('station', False),
                "stop": row.get('stop', False),
                "traffic_calming": row.get('traffic_calming', False),
                "traffic_signal": row.get('traffic_signal', False),
                "turning_loop": row.get('turning_loop', False)
            },
            "lighting_conditions": {
                "sunrise_sunset": row.get('sunrise_sunset'),
                "civil_twilight": row.get('civil_twilight'),
                "nautical_twilight": row.get('nautical_twilight'),
                "astronomical_twilight": row.get('astronomical_twilight')
            },
            "created_at": row.get('created_at'),
            "updated_at": row.get('updated_at')
        })
        
        return base_accident
    
    async def get_states_list(self) -> List[Dict[str, Any]]:
        """
        Récupère la liste des états avec statistiques
        
        Returns:
            list: Liste des états avec compteurs
        """
        try:
            self.logger.info("Fetching states list")
            
            query = f"""
            SELECT 
                state,
                COUNT(*) as accident_count,
                AVG(severity) as avg_severity,
                AVG(safety_score) as avg_safety_score
            FROM {self.accidents_table}
            GROUP BY state
            ORDER BY accident_count DESC
            """
            
            result = self.db_connector.execute_query(query)
            
            states = []
            for row in result:
                states.append({
                    "state": row['state'],
                    "accident_count": row['accident_count'],
                    "avg_severity": round(row['avg_severity'], 2),
                    "avg_safety_score": round(row['avg_safety_score'], 2)
                })
            
            self.logger.info("States list fetched successfully", states_count=len(states))
            return states
            
        except Exception as e:
            self.logger.error("Failed to fetch states list", exception=e)
            raise