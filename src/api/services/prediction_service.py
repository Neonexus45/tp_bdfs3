"""
Service pour les prédictions ML
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import time
import asyncio

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...applications.mltraining.prediction_service import PredictionService as MLPredictionService
from ..models.prediction_models import PredictionRequest, PredictionResponse, BatchPredictionRequest


class APIPredictionService:
    """Service de prédiction pour l'API FastAPI"""
    
    def __init__(self, config_manager: ConfigManager = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("api_prediction_service", self.config_manager)
        
        # Initialisation du service ML (lazy loading)
        self.ml_prediction_service = None
        self.mlflow_manager = None
        self._ml_initialized = False
        
        # Configuration du service
        self.default_model = "random_forest"
        self.model_cache = {}
        self.prediction_cache = {}
        self.cache_ttl = 300  # 5 minutes
        
        # Métriques du service
        self.service_metrics = {
            'total_predictions': 0,
            'successful_predictions': 0,
            'failed_predictions': 0,
            'avg_prediction_time_ms': 0.0,
            'cache_hits': 0,
            'cache_misses': 0
        }
    
    async def initialize_ml_service(self):
        """Initialise le service ML de prédiction"""
        if not self._ml_initialized:
            try:
                self.logger.info("Initializing ML prediction service")
                
                # Initialisation lazy de MLflow
                if self.mlflow_manager is None:
                    from ...applications.mltraining.mlflow_manager import MLflowManager
                    self.mlflow_manager = MLflowManager(self.config_manager)
                
                # Initialisation du service ML
                if self.ml_prediction_service is None:
                    # Note: Dans une vraie implémentation, on initialiserait Spark ici
                    # Pour cette démo, on simule l'initialisation
                    self.ml_prediction_service = MLPredictionService(
                        config_manager=self.config_manager,
                        spark=None  # Spark serait initialisé ici
                    )
                    
                    # Chargement du modèle par défaut
                    await self._load_default_model()
                
                self._ml_initialized = True
                self.logger.info("ML prediction service initialized successfully")
            
            except Exception as e:
                self.logger.error("Failed to initialize ML prediction service", exception=e)
                # Ne pas lever l'exception pour permettre à l'API de fonctionner sans ML
                self.logger.warning("API will continue without ML prediction capabilities")
    
    async def _load_default_model(self):
        """Charge le modèle par défaut"""
        try:
            # Dans une vraie implémentation, on chargerait depuis MLflow
            self.logger.info("Loading default model", model_name=self.default_model)
            
            # Simulation du chargement de modèle
            self.model_cache[self.default_model] = {
                'name': 'random_forest_severity',
                'version': 'v1.2.0',
                'loaded_at': datetime.now(),
                'accuracy': 0.87,
                'features': [
                    'Distance_mi', 'Temperature_F', 'Humidity_percent',
                    'Pressure_in', 'Visibility_mi', 'Start_Lat', 'Start_Lng',
                    'accident_hour', 'accident_day_of_week', 'infrastructure_count'
                ]
            }
            
            self.logger.info("Default model loaded successfully")
            
        except Exception as e:
            self.logger.error("Failed to load default model", exception=e)
            raise
    
    async def predict_severity(
        self,
        request: PredictionRequest,
        model_name: str = None
    ) -> PredictionResponse:
        """
        Effectue une prédiction de sévérité d'accident
        
        Args:
            request: Requête de prédiction
            model_name: Nom du modèle à utiliser
        
        Returns:
            PredictionResponse: Résultat de la prédiction
        """
        start_time = time.time()
        
        try:
            # Initialisation du service ML si nécessaire
            if self.ml_prediction_service is None:
                await self.initialize_ml_service()
            
            model_name = model_name or self.default_model
            
            self.logger.info("Starting severity prediction", 
                           model_name=model_name,
                           state=request.state,
                           lat=request.start_lat,
                           lng=request.start_lng)
            
            # Vérification du cache
            cache_key = self._generate_cache_key(request, model_name)
            cached_result = self._get_from_cache(cache_key)
            
            if cached_result:
                self.service_metrics['cache_hits'] += 1
                self.logger.debug("Prediction served from cache")
                return cached_result
            
            self.service_metrics['cache_misses'] += 1
            
            # Préparation des données d'entrée
            input_data = self._prepare_input_data(request)
            
            # Prédiction (simulée pour cette démo)
            prediction_result = await self._perform_prediction(input_data, model_name)
            
            # Construction de la réponse
            response = self._build_prediction_response(
                prediction_result, 
                model_name, 
                start_time,
                request
            )
            
            # Mise en cache du résultat
            self._cache_result(cache_key, response)
            
            # Mise à jour des métriques
            self.service_metrics['total_predictions'] += 1
            self.service_metrics['successful_predictions'] += 1
            
            processing_time = (time.time() - start_time) * 1000
            self._update_avg_time(processing_time)
            
            self.logger.info("Severity prediction completed successfully",
                           predicted_severity=response.predicted_severity,
                           confidence=response.confidence_score,
                           processing_time_ms=processing_time)
            
            return response
            
        except Exception as e:
            self.service_metrics['failed_predictions'] += 1
            self.logger.error("Severity prediction failed", exception=e)
            raise
    
    async def predict_batch(
        self,
        request: BatchPredictionRequest
    ) -> Dict[str, Any]:
        """
        Effectue des prédictions en lot
        
        Args:
            request: Requête de prédictions en lot
        
        Returns:
            dict: Résultats des prédictions en lot
        """
        start_time = time.time()
        
        try:
            self.logger.info("Starting batch prediction", 
                           batch_size=len(request.predictions),
                           model_name=request.model_name)
            
            predictions = []
            failed_predictions = 0
            
            # Traitement de chaque prédiction
            for i, pred_request in enumerate(request.predictions):
                try:
                    prediction = await self.predict_severity(pred_request, request.model_name)
                    predictions.append(prediction)
                    
                except Exception as e:
                    failed_predictions += 1
                    self.logger.warning(f"Prediction {i} failed", exception=e)
                    
                    # Prédiction par défaut en cas d'erreur
                    default_prediction = self._create_default_prediction(pred_request)
                    predictions.append(default_prediction)
            
            # Calcul du résumé du lot
            batch_summary = self._calculate_batch_summary(predictions)
            
            total_time = (time.time() - start_time) * 1000
            
            response = {
                "predictions": [pred.dict() for pred in predictions],
                "batch_summary": batch_summary,
                "total_processing_time_ms": total_time,
                "model_used": request.model_name or self.default_model,
                "batch_timestamp": datetime.now(),
                "failed_predictions": failed_predictions
            }
            
            self.logger.info("Batch prediction completed",
                           total_predictions=len(predictions),
                           failed_predictions=failed_predictions,
                           total_time_ms=total_time)
            
            return response
            
        except Exception as e:
            self.logger.error("Batch prediction failed", exception=e)
            raise
    
    async def get_available_models(self) -> List[Dict[str, Any]]:
        """
        Récupère la liste des modèles disponibles
        
        Returns:
            list: Liste des modèles disponibles
        """
        try:
            self.logger.info("Fetching available models")
            
            # Dans une vraie implémentation, on interrogerait MLflow Registry
            models = [
                {
                    "name": "random_forest_severity",
                    "version": "v1.2.0",
                    "description": "Modèle Random Forest pour prédiction de sévérité",
                    "accuracy": 0.87,
                    "training_date": datetime(2023, 11, 15, 9, 0, 0),
                    "features_count": 47,
                    "is_active": True
                },
                {
                    "name": "gradient_boosting_severity",
                    "version": "v1.1.0",
                    "description": "Modèle Gradient Boosting pour prédiction de sévérité",
                    "accuracy": 0.84,
                    "training_date": datetime(2023, 10, 20, 14, 30, 0),
                    "features_count": 47,
                    "is_active": False
                },
                {
                    "name": "neural_network_severity",
                    "version": "v2.0.0",
                    "description": "Réseau de neurones pour prédiction de sévérité",
                    "accuracy": 0.89,
                    "training_date": datetime(2023, 12, 1, 10, 0, 0),
                    "features_count": 52,
                    "is_active": False
                }
            ]
            
            self.logger.info("Available models fetched", count=len(models))
            return models
            
        except Exception as e:
            self.logger.error("Failed to fetch available models", exception=e)
            raise
    
    def _prepare_input_data(self, request: PredictionRequest) -> Dict[str, Any]:
        """
        Prépare les données d'entrée pour la prédiction
        
        Args:
            request: Requête de prédiction
        
        Returns:
            dict: Données préparées
        """
        # Conversion des données Pydantic en dictionnaire
        input_data = request.dict()
        
        # Ajout de features dérivées
        if request.accident_hour is None:
            input_data['accident_hour'] = 12  # Valeur par défaut
        
        if request.accident_day_of_week is None:
            input_data['accident_day_of_week'] = 2  # Mardi par défaut
        
        # Calcul de features booléennes pour l'infrastructure
        infrastructure_count = 0
        for field in ['amenity', 'bump', 'crossing', 'give_way', 'junction', 
                     'no_exit', 'railway', 'roundabout', 'station', 'stop', 
                     'traffic_calming', 'traffic_signal', 'turning_loop']:
            if input_data.get(field):
                infrastructure_count += 1
        
        input_data['infrastructure_count'] = infrastructure_count
        
        # Ajout de features temporelles dérivées
        hour = input_data.get('accident_hour', 12)
        input_data['is_rush_hour'] = hour in [7, 8, 17, 18, 19]
        
        day_of_week = input_data.get('accident_day_of_week', 2)
        input_data['is_weekend'] = day_of_week in [5, 6]  # Samedi, Dimanche
        
        return input_data
    
    async def _perform_prediction(
        self, 
        input_data: Dict[str, Any], 
        model_name: str
    ) -> Dict[str, Any]:
        """
        Effectue la prédiction avec le modèle ML
        
        Args:
            input_data: Données d'entrée préparées
            model_name: Nom du modèle
        
        Returns:
            dict: Résultat de la prédiction
        """
        # Simulation de prédiction (dans une vraie implémentation, 
        # on utiliserait le service ML réel)
        
        # Calcul d'un score basé sur les features
        danger_factors = 0
        
        # Facteurs géographiques
        if input_data.get('state') in ['CA', 'TX', 'FL']:
            danger_factors += 0.3
        
        # Facteurs météorologiques
        if input_data.get('weather_condition') in ['Rain', 'Snow', 'Fog']:
            danger_factors += 0.4
        
        if input_data.get('visibility_miles', 10) < 5:
            danger_factors += 0.3
        
        # Facteurs temporels
        if input_data.get('is_rush_hour'):
            danger_factors += 0.2
        
        # Facteurs d'infrastructure
        infrastructure_count = input_data.get('infrastructure_count', 0)
        if infrastructure_count > 3:
            danger_factors += 0.2
        elif infrastructure_count == 0:
            danger_factors += 0.3
        
        # Calcul de la sévérité prédite
        if danger_factors >= 1.0:
            predicted_severity = 4
            probabilities = {"1": 0.05, "2": 0.15, "3": 0.30, "4": 0.50}
        elif danger_factors >= 0.7:
            predicted_severity = 3
            probabilities = {"1": 0.10, "2": 0.25, "3": 0.45, "4": 0.20}
        elif danger_factors >= 0.4:
            predicted_severity = 2
            probabilities = {"1": 0.20, "2": 0.50, "3": 0.25, "4": 0.05}
        else:
            predicted_severity = 1
            probabilities = {"1": 0.60, "2": 0.30, "3": 0.08, "4": 0.02}
        
        confidence_score = max(probabilities.values())
        
        return {
            'predicted_severity': predicted_severity,
            'probabilities': probabilities,
            'confidence_score': confidence_score,
            'danger_factors': danger_factors
        }
    
    def _build_prediction_response(
        self,
        prediction_result: Dict[str, Any],
        model_name: str,
        start_time: float,
        request: PredictionRequest
    ) -> PredictionResponse:
        """
        Construit la réponse de prédiction
        
        Args:
            prediction_result: Résultat de la prédiction
            model_name: Nom du modèle utilisé
            start_time: Temps de début
            request: Requête originale
        
        Returns:
            PredictionResponse: Réponse formatée
        """
        processing_time = (time.time() - start_time) * 1000
        
        severity_names = {1: "Minor", 2: "Moderate", 3: "Serious", 4: "Severe"}
        predicted_severity = prediction_result['predicted_severity']
        
        # Génération des facteurs de risque
        risk_factors = self._identify_risk_factors(request, prediction_result)
        
        # Génération des recommandations de sécurité
        safety_recommendations = self._generate_safety_recommendations(
            predicted_severity, 
            request
        )
        
        model_info = self.model_cache.get(model_name, {})
        
        return PredictionResponse(
            predicted_severity=predicted_severity,
            predicted_severity_name=severity_names[predicted_severity],
            confidence_score=prediction_result['confidence_score'],
            probabilities=prediction_result['probabilities'],
            model_used=model_info.get('name', model_name),
            model_version=model_info.get('version', 'v1.0.0'),
            prediction_timestamp=datetime.now(),
            processing_time_ms=processing_time,
            risk_factors=risk_factors,
            safety_recommendations=safety_recommendations
        )
    
    def _identify_risk_factors(
        self, 
        request: PredictionRequest, 
        prediction_result: Dict[str, Any]
    ) -> List[str]:
        """
        Identifie les facteurs de risque
        
        Args:
            request: Requête de prédiction
            prediction_result: Résultat de la prédiction
        
        Returns:
            list: Liste des facteurs de risque
        """
        risk_factors = []
        
        # Facteurs météorologiques
        if request.weather_condition in ['Rain', 'Snow', 'Fog']:
            risk_factors.append(f"Conditions météorologiques: {request.weather_condition}")
        
        if request.visibility_miles and request.visibility_miles < 5:
            risk_factors.append("Visibilité réduite")
        
        # Facteurs temporels
        if request.accident_hour and request.accident_hour in [7, 8, 17, 18, 19]:
            risk_factors.append("Heure de pointe")
        
        # Facteurs géographiques
        if request.state in ['CA', 'TX', 'FL']:
            risk_factors.append("État à forte densité d'accidents")
        
        # Facteurs d'infrastructure
        infrastructure_count = sum([
            bool(getattr(request, field, False)) 
            for field in ['junction', 'traffic_signal', 'roundabout', 'railway']
        ])
        
        if infrastructure_count > 2:
            risk_factors.append("Zone d'infrastructure complexe")
        elif infrastructure_count == 0:
            risk_factors.append("Absence d'infrastructure de sécurité")
        
        return risk_factors[:5]  # Limiter à 5 facteurs
    
    def _generate_safety_recommendations(
        self, 
        predicted_severity: int, 
        request: PredictionRequest
    ) -> List[str]:
        """
        Génère des recommandations de sécurité
        
        Args:
            predicted_severity: Sévérité prédite
            request: Requête de prédiction
        
        Returns:
            list: Liste des recommandations
        """
        recommendations = []
        
        if predicted_severity >= 3:
            recommendations.append("Réduire la vitesse de manière significative")
            recommendations.append("Maintenir une distance de sécurité accrue")
        
        if predicted_severity >= 2:
            recommendations.append("Être vigilant aux intersections")
            recommendations.append("Adapter la conduite aux conditions")
        
        # Recommandations spécifiques aux conditions
        if request.weather_condition in ['Rain', 'Snow']:
            recommendations.append("Adapter la vitesse aux conditions météo")
        
        if request.visibility_miles and request.visibility_miles < 5:
            recommendations.append("Utiliser les feux de brouillard")
        
        if request.accident_hour and request.accident_hour in [7, 8, 17, 18, 19]:
            recommendations.append("Éviter les manœuvres risquées en heure de pointe")
        
        recommendations.append("Rester concentré sur la conduite")
        
        return recommendations[:5]  # Limiter à 5 recommandations
    
    def _create_default_prediction(self, request: PredictionRequest) -> PredictionResponse:
        """
        Crée une prédiction par défaut en cas d'erreur
        
        Args:
            request: Requête de prédiction
        
        Returns:
            PredictionResponse: Prédiction par défaut
        """
        return PredictionResponse(
            predicted_severity=2,
            predicted_severity_name="Moderate",
            confidence_score=0.5,
            probabilities={"1": 0.25, "2": 0.50, "3": 0.20, "4": 0.05},
            model_used="default",
            model_version="v1.0.0",
            prediction_timestamp=datetime.now(),
            processing_time_ms=0.0,
            risk_factors=["Prédiction par défaut"],
            safety_recommendations=["Conduire prudemment"]
        )
    
    def _calculate_batch_summary(self, predictions: List[PredictionResponse]) -> Dict[str, Any]:
        """
        Calcule le résumé d'un lot de prédictions
        
        Args:
            predictions: Liste des prédictions
        
        Returns:
            dict: Résumé du lot
        """
        if not predictions:
            return {}
        
        total_predictions = len(predictions)
        avg_confidence = sum(p.confidence_score for p in predictions) / total_predictions
        
        severity_distribution = {"1": 0, "2": 0, "3": 0, "4": 0}
        for pred in predictions:
            severity_distribution[str(pred.predicted_severity)] += 1
        
        return {
            "total_predictions": total_predictions,
            "avg_confidence": round(avg_confidence, 3),
            "severity_distribution": severity_distribution,
            "high_risk_predictions": len([p for p in predictions if p.predicted_severity >= 3])
        }
    
    def _generate_cache_key(self, request: PredictionRequest, model_name: str) -> str:
        """Génère une clé de cache pour la requête"""
        key_data = f"{model_name}_{request.state}_{request.start_lat}_{request.start_lng}"
        key_data += f"_{request.weather_condition}_{request.accident_hour}"
        return key_data
    
    def _get_from_cache(self, cache_key: str) -> Optional[PredictionResponse]:
        """Récupère un résultat du cache"""
        cached_data = self.prediction_cache.get(cache_key)
        if cached_data and (datetime.now() - cached_data['timestamp']).seconds < self.cache_ttl:
            return cached_data['result']
        return None
    
    def _cache_result(self, cache_key: str, result: PredictionResponse):
        """Met en cache un résultat"""
        self.prediction_cache[cache_key] = {
            'result': result,
            'timestamp': datetime.now()
        }
    
    def _update_avg_time(self, processing_time: float):
        """Met à jour le temps moyen de traitement"""
        current_avg = self.service_metrics['avg_prediction_time_ms']
        total_predictions = self.service_metrics['total_predictions']
        
        if total_predictions > 0:
            self.service_metrics['avg_prediction_time_ms'] = (
                (current_avg * (total_predictions - 1) + processing_time) / total_predictions
            )
        else:
            self.service_metrics['avg_prediction_time_ms'] = processing_time
    
    def get_service_metrics(self) -> Dict[str, Any]:
        """Récupère les métriques du service"""
        return self.service_metrics.copy()
    
    async def health_check(self) -> Dict[str, Any]:
        """Vérifie la santé du service de prédiction"""
        try:
            health_status = {
                "status": "healthy",
                "ml_service_initialized": self.ml_prediction_service is not None,
                "models_loaded": len(self.model_cache),
                "cache_size": len(self.prediction_cache),
                "metrics": self.get_service_metrics(),
                "timestamp": datetime.now()
            }
            
            return health_status
            
        except Exception as e:
            self.logger.error("Health check failed", exception=e)
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now()
            }