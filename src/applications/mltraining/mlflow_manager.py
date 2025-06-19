"""
Composant MLflowManager pour la gestion complète de MLflow
"""

import time
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType
from pyspark.ml import Model

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import MLModelError, ConfigurationError


class MLflowManager:
    """
    Gestionnaire complet MLflow pour tracking et model registry
    
    Fonctionnalités:
    - Tracking: Logging des expériences, paramètres, métriques et artifacts
    - Model Registry: Versioning et staging des modèles (Staging, Production, Archived)
    - Artifacts: Sauvegarde modèles, visualisations et métadonnées
    - Metadata: Informations détaillées sur les runs et modèles
    - Comparaison: Analyse comparative des expériences
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le gestionnaire MLflow"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("mlflow_manager", self.config_manager)
        
        # Configuration MLflow
        self.tracking_uri = self.config_manager.get('mlflow.tracking_uri')
        self.experiment_name = self.config_manager.get('mlflow.experiment_name')
        self.artifact_root = self.config_manager.get('mlflow.artifact_root')
        
        # Client MLflow
        self.client = None
        self.current_run_id = None
        self.experiment_id = None
        
        # Métriques du gestionnaire
        self.manager_metrics = {
            'runs_created': 0,
            'models_logged': 0,
            'experiments_created': 0,
            'artifacts_logged': 0,
            'models_registered': 0
        }
        
        # Initialisation
        self._initialize_mlflow()
    
    def _initialize_mlflow(self) -> None:
        """Initialise la connexion MLflow"""
        try:
            self.logger.info("Initializing MLflow connection", tracking_uri=self.tracking_uri)
            
            # Configuration de l'URI de tracking
            mlflow.set_tracking_uri(self.tracking_uri)
            
            # Création du client
            self.client = MlflowClient(tracking_uri=self.tracking_uri)
            
            # Création ou récupération de l'expérience
            self._setup_experiment()
            
            self.logger.info("MLflow initialized successfully",
                           experiment_name=self.experiment_name,
                           experiment_id=self.experiment_id)
            
        except Exception as e:
            self.logger.error("MLflow initialization failed", exception=e)
            raise ConfigurationError(
                message="MLflow initialization failed",
                config_key="mlflow.tracking_uri",
                config_value=self.tracking_uri
            )
    
    def _setup_experiment(self) -> None:
        """Configure l'expérience MLflow"""
        try:
            # Tentative de récupération de l'expérience existante
            try:
                experiment = self.client.get_experiment_by_name(self.experiment_name)
                if experiment:
                    self.experiment_id = experiment.experiment_id
                    self.logger.info("Using existing experiment", 
                                   experiment_id=self.experiment_id)
                    return
            except Exception:
                pass
            
            # Création d'une nouvelle expérience
            self.experiment_id = self.client.create_experiment(
                name=self.experiment_name,
                artifact_location=self.artifact_root
            )
            
            self.manager_metrics['experiments_created'] += 1
            self.logger.info("Created new experiment",
                           experiment_name=self.experiment_name,
                           experiment_id=self.experiment_id)
            
        except Exception as e:
            self.logger.error("Experiment setup failed", exception=e)
            raise MLModelError(
                message="MLflow experiment setup failed",
                operation="experiment_setup"
            )
    
    def start_experiment_run(self, experiment_name: str = None, run_name: str = None,
                           tags: Dict[str, str] = None) -> str:
        """Démarre un nouveau run d'expérience"""
        try:
            # Utilisation de l'expérience par défaut si non spécifiée
            if experiment_name and experiment_name != self.experiment_name:
                mlflow.set_experiment(experiment_name)
            else:
                mlflow.set_experiment(self.experiment_name)
            
            # Tags par défaut
            default_tags = {
                'mlflow.runName': run_name or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'mlflow.source.type': 'JOB',
                'application': 'mltraining',
                'environment': self.config_manager.get('environment', 'development')
            }
            
            if tags:
                default_tags.update(tags)
            
            # Démarrage du run
            run = mlflow.start_run(tags=default_tags)
            self.current_run_id = run.info.run_id
            
            self.manager_metrics['runs_created'] += 1
            
            self.logger.info("MLflow run started",
                           run_id=self.current_run_id,
                           run_name=run_name,
                           experiment_name=experiment_name or self.experiment_name)
            
            return self.current_run_id
            
        except Exception as e:
            self.logger.error("Failed to start MLflow run", exception=e)
            raise MLModelError(
                message="MLflow run start failed",
                operation="start_run"
            )
    
    def log_params(self, params: Dict[str, Any]) -> None:
        """Log les paramètres du modèle"""
        try:
            if not self.current_run_id:
                raise MLModelError(
                    message="No active MLflow run",
                    operation="log_params"
                )
            
            # Conversion des paramètres en strings si nécessaire
            string_params = {}
            for key, value in params.items():
                if isinstance(value, (dict, list)):
                    string_params[key] = json.dumps(value)
                else:
                    string_params[key] = str(value)
            
            mlflow.log_params(string_params)
            
            self.logger.debug("Parameters logged to MLflow", params_count=len(params))
            
        except Exception as e:
            self.logger.error("Failed to log parameters", exception=e)
            raise MLModelError(
                message="Parameter logging failed",
                operation="log_params"
            )
    
    def log_metrics(self, metrics: Dict[str, Union[int, float]], step: int = None) -> None:
        """Log les métriques du modèle"""
        try:
            if not self.current_run_id:
                raise MLModelError(
                    message="No active MLflow run",
                    operation="log_metrics"
                )
            
            # Filtrage des métriques numériques
            numeric_metrics = {}
            for key, value in metrics.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    numeric_metrics[key] = float(value)
            
            if step is not None:
                for key, value in numeric_metrics.items():
                    mlflow.log_metric(key, value, step=step)
            else:
                mlflow.log_metrics(numeric_metrics)
            
            self.logger.debug("Metrics logged to MLflow", metrics_count=len(numeric_metrics))
            
        except Exception as e:
            self.logger.error("Failed to log metrics", exception=e)
            raise MLModelError(
                message="Metrics logging failed",
                operation="log_metrics"
            )
    
    def log_artifacts(self, artifacts: Dict[str, str]) -> None:
        """Log les artifacts (fichiers, plots, etc.)"""
        try:
            if not self.current_run_id:
                raise MLModelError(
                    message="No active MLflow run",
                    operation="log_artifacts"
                )
            
            artifacts_logged = 0
            for artifact_name, artifact_path in artifacts.items():
                try:
                    mlflow.log_artifact(artifact_path, artifact_name)
                    artifacts_logged += 1
                except Exception as artifact_e:
                    self.logger.warning(f"Failed to log artifact {artifact_name}", 
                                      exception=artifact_e)
            
            self.manager_metrics['artifacts_logged'] += artifacts_logged
            self.logger.debug("Artifacts logged to MLflow", artifacts_count=artifacts_logged)
            
        except Exception as e:
            self.logger.error("Failed to log artifacts", exception=e)
            raise MLModelError(
                message="Artifacts logging failed",
                operation="log_artifacts"
            )
    
    def save_model(self, model: Model, model_name: str, feature_columns: List[str],
                   metrics: Dict[str, Any], feature_info: Dict[str, Any]) -> str:
        """Sauvegarde un modèle dans MLflow avec métadonnées"""
        try:
            if not self.current_run_id:
                raise MLModelError(
                    message="No active MLflow run",
                    operation="save_model"
                )
            
            self.logger.info("Saving model to MLflow", model_name=model_name)
            
            # Métadonnées du modèle
            model_metadata = {
                'model_name': model_name,
                'feature_columns': feature_columns,
                'feature_count': len(feature_columns),
                'model_type': type(model).__name__,
                'training_timestamp': datetime.now().isoformat(),
                'performance_metrics': metrics,
                'feature_processing_info': feature_info
            }
            
            # Sauvegarde du modèle Spark
            mlflow.spark.log_model(
                spark_model=model,
                artifact_path="model",
                registered_model_name=model_name
            )
            
            # Log des métadonnées
            mlflow.log_dict(model_metadata, "model_metadata.json")
            
            # Log des colonnes de features
            mlflow.log_dict(
                {'feature_columns': feature_columns}, 
                "feature_columns.json"
            )
            
            self.manager_metrics['models_logged'] += 1
            
            # Enregistrement dans le Model Registry
            model_version = self._register_model(model_name, self.current_run_id)
            
            self.logger.info("Model saved successfully",
                           model_name=model_name,
                           model_version=model_version,
                           run_id=self.current_run_id)
            
            return model_version
            
        except Exception as e:
            self.logger.error("Model saving failed", exception=e, model_name=model_name)
            raise MLModelError(
                message=f"Model saving failed for {model_name}",
                model_name=model_name,
                operation="save_model"
            )
    
    def _register_model(self, model_name: str, run_id: str) -> str:
        """Enregistre le modèle dans le Model Registry"""
        try:
            # Création de la version du modèle
            model_uri = f"runs:/{run_id}/model"
            
            model_version = mlflow.register_model(
                model_uri=model_uri,
                name=model_name
            )
            
            # Transition vers Staging
            self.client.transition_model_version_stage(
                name=model_name,
                version=model_version.version,
                stage="Staging"
            )
            
            self.manager_metrics['models_registered'] += 1
            
            self.logger.info("Model registered in Model Registry",
                           model_name=model_name,
                           version=model_version.version,
                           stage="Staging")
            
            return model_version.version
            
        except Exception as e:
            self.logger.error("Model registration failed", exception=e)
            raise MLModelError(
                message="Model registration failed",
                model_name=model_name,
                operation="register_model"
            )
    
    def load_model(self, model_name: str, version: str = None, stage: str = None) -> Model:
        """Charge un modèle depuis le Model Registry"""
        try:
            if version:
                model_uri = f"models:/{model_name}/{version}"
            elif stage:
                model_uri = f"models:/{model_name}/{stage}"
            else:
                model_uri = f"models:/{model_name}/latest"
            
            model = mlflow.spark.load_model(model_uri)
            
            self.logger.info("Model loaded from MLflow",
                           model_name=model_name,
                           version=version,
                           stage=stage)
            
            return model
            
        except Exception as e:
            self.logger.error("Model loading failed", exception=e, model_name=model_name)
            raise MLModelError(
                message=f"Model loading failed for {model_name}",
                model_name=model_name,
                operation="load_model"
            )
    
    def promote_model_to_production(self, model_name: str, version: str) -> None:
        """Promeut un modèle vers la production"""
        try:
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage="Production"
            )
            
            self.logger.info("Model promoted to production",
                           model_name=model_name,
                           version=version)
            
        except Exception as e:
            self.logger.error("Model promotion failed", exception=e)
            raise MLModelError(
                message="Model promotion to production failed",
                model_name=model_name,
                operation="promote_model"
            )
    
    def archive_model(self, model_name: str, version: str) -> None:
        """Archive un modèle"""
        try:
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage="Archived"
            )
            
            self.logger.info("Model archived",
                           model_name=model_name,
                           version=version)
            
        except Exception as e:
            self.logger.error("Model archiving failed", exception=e)
            raise MLModelError(
                message="Model archiving failed",
                model_name=model_name,
                operation="archive_model"
            )
    
    def get_experiment_runs(self, experiment_name: str = None, 
                          max_results: int = 100) -> List[Dict[str, Any]]:
        """Récupère les runs d'une expérience"""
        try:
            exp_name = experiment_name or self.experiment_name
            experiment = self.client.get_experiment_by_name(exp_name)
            
            if not experiment:
                return []
            
            runs = self.client.search_runs(
                experiment_ids=[experiment.experiment_id],
                run_view_type=ViewType.ACTIVE_ONLY,
                max_results=max_results,
                order_by=["start_time DESC"]
            )
            
            runs_data = []
            for run in runs:
                run_data = {
                    'run_id': run.info.run_id,
                    'run_name': run.data.tags.get('mlflow.runName', 'Unknown'),
                    'status': run.info.status,
                    'start_time': run.info.start_time,
                    'end_time': run.info.end_time,
                    'metrics': dict(run.data.metrics),
                    'params': dict(run.data.params),
                    'tags': dict(run.data.tags)
                }
                runs_data.append(run_data)
            
            self.logger.info("Retrieved experiment runs",
                           experiment_name=exp_name,
                           runs_count=len(runs_data))
            
            return runs_data
            
        except Exception as e:
            self.logger.error("Failed to get experiment runs", exception=e)
            return []
    
    def compare_runs(self, run_ids: List[str]) -> Dict[str, Any]:
        """Compare plusieurs runs"""
        try:
            comparison_data = {
                'runs': [],
                'metrics_comparison': {},
                'params_comparison': {}
            }
            
            for run_id in run_ids:
                try:
                    run = self.client.get_run(run_id)
                    run_data = {
                        'run_id': run_id,
                        'run_name': run.data.tags.get('mlflow.runName', 'Unknown'),
                        'metrics': dict(run.data.metrics),
                        'params': dict(run.data.params),
                        'status': run.info.status
                    }
                    comparison_data['runs'].append(run_data)
                except Exception as run_e:
                    self.logger.warning(f"Failed to get run {run_id}", exception=run_e)
            
            # Comparaison des métriques
            if comparison_data['runs']:
                all_metrics = set()
                for run in comparison_data['runs']:
                    all_metrics.update(run['metrics'].keys())
                
                for metric in all_metrics:
                    comparison_data['metrics_comparison'][metric] = {}
                    for run in comparison_data['runs']:
                        comparison_data['metrics_comparison'][metric][run['run_id']] = \
                            run['metrics'].get(metric, None)
            
            self.logger.info("Runs comparison completed", runs_count=len(comparison_data['runs']))
            return comparison_data
            
        except Exception as e:
            self.logger.error("Runs comparison failed", exception=e)
            return {}
    
    def end_run(self, status: str = 'FINISHED') -> None:
        """Termine le run actuel"""
        try:
            if self.current_run_id:
                mlflow.end_run(status=status)
                
                self.logger.info("MLflow run ended",
                               run_id=self.current_run_id,
                               status=status)
                
                self.current_run_id = None
            
        except Exception as e:
            self.logger.error("Failed to end MLflow run", exception=e)
    
    def get_manager_summary(self) -> Dict[str, Any]:
        """Retourne un résumé du gestionnaire MLflow"""
        return {
            'tracking_uri': self.tracking_uri,
            'experiment_name': self.experiment_name,
            'experiment_id': self.experiment_id,
            'current_run_id': self.current_run_id,
            'manager_metrics': self.manager_metrics
        }