"""
Composant HyperparameterTuner pour l'optimisation des hyperparamètres
"""

import time
from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.classification import (
    RandomForestClassifier, GBTClassifier, LogisticRegression, DecisionTreeClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit
from pyspark.ml import Pipeline
import itertools

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import MLModelError, SparkJobError


class HyperparameterTuner:
    """
    Composant d'optimisation des hyperparamètres pour modèles ML
    
    Méthodes supportées:
    - Grid Search: Recherche exhaustive dans une grille de paramètres
    - Random Search: Recherche aléatoire efficace
    - Cross-validation: Validation croisée k-fold
    - Train-Validation Split: Division simple pour datasets volumineux
    
    Optimisation basée sur F1-score macro pour classification déséquilibrée
    """
    
    def __init__(self, config_manager: ConfigManager = None, spark: SparkSession = None):
        """Initialise le composant HyperparameterTuner"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("hyperparameter_tuner", self.config_manager)
        self.spark = spark
        
        # Configuration du tuning
        self.cv_folds = 3  # Cross-validation folds
        self.train_ratio = 0.8  # Pour TrainValidationSplit
        self.random_seed = 42
        self.max_evals = 20  # Nombre maximum d'évaluations pour random search
        
        # Grilles de paramètres pour chaque modèle
        self.param_grids = {
            'random_forest': {
                'numTrees': [50, 100, 200],
                'maxDepth': [10, 15, 20],
                'minInstancesPerNode': [1, 5, 10],
                'subsamplingRate': [0.8, 1.0]
            },
            'gradient_boosting': {
                'maxIter': [50, 100, 150],
                'maxDepth': [5, 10, 15],
                'stepSize': [0.01, 0.1, 0.2],
                'subsamplingRate': [0.8, 1.0]
            },
            'logistic_regression': {
                'regParam': [0.01, 0.1, 1.0],
                'elasticNetParam': [0.0, 0.5, 1.0],
                'maxIter': [100, 200]
            },
            'decision_tree': {
                'maxDepth': [10, 15, 20, 25],
                'minInstancesPerNode': [1, 5, 10],
                'minInfoGain': [0.0, 0.01, 0.1]
            }
        }
        
        # Métriques de tuning
        self.tuning_metrics = {
            'tuning_sessions': 0,
            'total_tuning_time': 0,
            'best_scores_achieved': {},
            'parameter_combinations_tested': 0
        }
    
    def tune_model(self, model_name: str, train_df: DataFrame, validation_df: DataFrame,
                   feature_columns: List[str], target_column: str,
                   tuning_method: str = "grid_search") -> Tuple[Any, Dict[str, Any]]:
        """Optimise les hyperparamètres d'un modèle"""
        try:
            self.logger.info("Starting hyperparameter tuning",
                           model_name=model_name,
                           tuning_method=tuning_method)
            
            start_time = time.time()
            
            # Sélection de la méthode de tuning
            if tuning_method == "grid_search":
                best_model, tuning_results = self._grid_search_tuning(
                    model_name, train_df, validation_df, feature_columns, target_column
                )
            elif tuning_method == "random_search":
                best_model, tuning_results = self._random_search_tuning(
                    model_name, train_df, validation_df, feature_columns, target_column
                )
            elif tuning_method == "cross_validation":
                best_model, tuning_results = self._cross_validation_tuning(
                    model_name, train_df, feature_columns, target_column
                )
            else:
                raise MLModelError(
                    message=f"Unknown tuning method: {tuning_method}",
                    model_name=model_name,
                    operation="hyperparameter_tuning"
                )
            
            tuning_time = time.time() - start_time
            
            # Mise à jour des métriques
            self.tuning_metrics['tuning_sessions'] += 1
            self.tuning_metrics['total_tuning_time'] += tuning_time
            self.tuning_metrics['best_scores_achieved'][model_name] = tuning_results['best_score']
            
            # Ajout des informations de timing
            tuning_results.update({
                'tuning_time_seconds': tuning_time,
                'tuning_method': tuning_method,
                'model_name': model_name
            })
            
            self.logger.log_performance(
                operation="hyperparameter_tuning",
                duration_seconds=tuning_time,
                model_name=model_name,
                tuning_method=tuning_method,
                best_score=tuning_results['best_score'],
                combinations_tested=tuning_results.get('combinations_tested', 0)
            )
            
            return best_model, tuning_results
            
        except Exception as e:
            self.logger.error("Hyperparameter tuning failed", exception=e, 
                            model_name=model_name, tuning_method=tuning_method)
            raise MLModelError(
                message=f"Hyperparameter tuning failed for {model_name}",
                model_name=model_name,
                operation="hyperparameter_tuning"
            )
    
    def _grid_search_tuning(self, model_name: str, train_df: DataFrame, validation_df: DataFrame,
                           feature_columns: List[str], target_column: str) -> Tuple[Any, Dict[str, Any]]:
        """Effectue un grid search pour optimiser les hyperparamètres"""
        try:
            self.logger.info("Performing grid search tuning", model_name=model_name)
            
            # Création du modèle de base
            base_model = self._create_base_model(model_name, target_column)
            
            # Construction de la grille de paramètres
            param_grid = self._build_param_grid(model_name, base_model)
            
            # Évaluateur
            evaluator = MulticlassClassificationEvaluator(
                labelCol=target_column,
                predictionCol="prediction",
                metricName="f1"  # F1-score macro
            )
            
            # CrossValidator
            cv = CrossValidator(
                estimator=base_model,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                numFolds=self.cv_folds,
                seed=self.random_seed
            )
            
            # Entraînement avec cross-validation
            cv_model = cv.fit(train_df)
            
            # Meilleur modèle
            best_model = cv_model.bestModel
            
            # Évaluation sur validation set
            validation_predictions = best_model.transform(validation_df)
            best_score = evaluator.evaluate(validation_predictions)
            
            # Extraction des meilleurs paramètres
            best_params = self._extract_best_params(best_model, model_name)
            
            # Métriques de validation croisée
            cv_scores = cv_model.avgMetrics
            
            tuning_results = {
                'best_score': round(best_score, 4),
                'best_params': best_params,
                'cv_scores': [round(score, 4) for score in cv_scores],
                'cv_mean_score': round(sum(cv_scores) / len(cv_scores), 4),
                'cv_std_score': round(self._calculate_std(cv_scores), 4),
                'combinations_tested': len(param_grid)
            }
            
            self.tuning_metrics['parameter_combinations_tested'] += len(param_grid)
            
            self.logger.info("Grid search completed",
                           best_score=best_score,
                           combinations_tested=len(param_grid))
            
            return best_model, tuning_results
            
        except Exception as e:
            self.logger.error("Grid search tuning failed", exception=e)
            raise MLModelError(
                message="Grid search tuning failed",
                model_name=model_name,
                operation="grid_search"
            )
    
    def _random_search_tuning(self, model_name: str, train_df: DataFrame, validation_df: DataFrame,
                            feature_columns: List[str], target_column: str) -> Tuple[Any, Dict[str, Any]]:
        """Effectue un random search pour optimiser les hyperparamètres"""
        try:
            self.logger.info("Performing random search tuning", model_name=model_name)
            
            # Génération de combinaisons aléatoires de paramètres
            param_combinations = self._generate_random_params(model_name, self.max_evals)
            
            best_model = None
            best_score = 0.0
            best_params = {}
            all_scores = []
            
            # Évaluateur
            evaluator = MulticlassClassificationEvaluator(
                labelCol=target_column,
                predictionCol="prediction",
                metricName="f1"
            )
            
            # Test de chaque combinaison
            for i, params in enumerate(param_combinations):
                try:
                    self.logger.debug(f"Testing parameter combination {i+1}/{len(param_combinations)}")
                    
                    # Création et entraînement du modèle
                    model = self._create_model_with_params(model_name, target_column, params)
                    trained_model = model.fit(train_df)
                    
                    # Évaluation
                    predictions = trained_model.transform(validation_df)
                    score = evaluator.evaluate(predictions)
                    all_scores.append(score)
                    
                    # Mise à jour du meilleur modèle
                    if score > best_score:
                        best_score = score
                        best_model = trained_model
                        best_params = params.copy()
                    
                except Exception as param_e:
                    self.logger.warning(f"Parameter combination {i+1} failed", exception=param_e)
                    all_scores.append(0.0)
                    continue
            
            tuning_results = {
                'best_score': round(best_score, 4),
                'best_params': best_params,
                'all_scores': [round(score, 4) for score in all_scores],
                'mean_score': round(sum(all_scores) / len(all_scores), 4),
                'std_score': round(self._calculate_std(all_scores), 4),
                'combinations_tested': len(param_combinations)
            }
            
            self.tuning_metrics['parameter_combinations_tested'] += len(param_combinations)
            
            self.logger.info("Random search completed",
                           best_score=best_score,
                           combinations_tested=len(param_combinations))
            
            return best_model, tuning_results
            
        except Exception as e:
            self.logger.error("Random search tuning failed", exception=e)
            raise MLModelError(
                message="Random search tuning failed",
                model_name=model_name,
                operation="random_search"
            )
    
    def _cross_validation_tuning(self, model_name: str, train_df: DataFrame,
                               feature_columns: List[str], target_column: str) -> Tuple[Any, Dict[str, Any]]:
        """Effectue une validation croisée avec les paramètres par défaut"""
        try:
            self.logger.info("Performing cross-validation tuning", model_name=model_name)
            
            # Modèle avec paramètres par défaut
            model = self._create_base_model(model_name, target_column)
            
            # Évaluateur
            evaluator = MulticlassClassificationEvaluator(
                labelCol=target_column,
                predictionCol="prediction",
                metricName="f1"
            )
            
            # CrossValidator avec un seul jeu de paramètres
            param_grid = ParamGridBuilder().build()  # Grille vide = paramètres par défaut
            
            cv = CrossValidator(
                estimator=model,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                numFolds=self.cv_folds,
                seed=self.random_seed
            )
            
            # Entraînement
            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel
            
            # Scores de validation croisée
            cv_scores = cv_model.avgMetrics
            best_score = cv_scores[0] if cv_scores else 0.0
            
            # Paramètres utilisés (par défaut)
            default_params = self._get_default_params(model_name)
            
            tuning_results = {
                'best_score': round(best_score, 4),
                'best_params': default_params,
                'cv_scores': [round(score, 4) for score in cv_scores],
                'cv_mean_score': round(best_score, 4),
                'cv_std_score': 0.0,  # Un seul jeu de paramètres
                'combinations_tested': 1
            }
            
            self.logger.info("Cross-validation completed", best_score=best_score)
            
            return best_model, tuning_results
            
        except Exception as e:
            self.logger.error("Cross-validation tuning failed", exception=e)
            raise MLModelError(
                message="Cross-validation tuning failed",
                model_name=model_name,
                operation="cross_validation"
            )
    
    def _create_base_model(self, model_name: str, target_column: str):
        """Crée un modèle de base selon le type"""
        if model_name == 'random_forest':
            return RandomForestClassifier(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                probabilityCol="probability",
                seed=self.random_seed
            )
        elif model_name == 'gradient_boosting':
            return GBTClassifier(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                seed=self.random_seed
            )
        elif model_name == 'logistic_regression':
            return LogisticRegression(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                probabilityCol="probability",
                family="multinomial",
                seed=self.random_seed
            )
        elif model_name == 'decision_tree':
            return DecisionTreeClassifier(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                probabilityCol="probability",
                seed=self.random_seed
            )
        else:
            raise MLModelError(
                message=f"Unknown model name: {model_name}",
                model_name=model_name,
                operation="model_creation"
            )
    
    def _build_param_grid(self, model_name: str, model):
        """Construit la grille de paramètres pour un modèle"""
        param_grid_builder = ParamGridBuilder()
        
        if model_name not in self.param_grids:
            return param_grid_builder.build()
        
        param_config = self.param_grids[model_name]
        
        if model_name == 'random_forest':
            if 'numTrees' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.numTrees, param_config['numTrees'])
            if 'maxDepth' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.maxDepth, param_config['maxDepth'])
            if 'minInstancesPerNode' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.minInstancesPerNode, param_config['minInstancesPerNode'])
            if 'subsamplingRate' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.subsamplingRate, param_config['subsamplingRate'])
        
        elif model_name == 'gradient_boosting':
            if 'maxIter' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.maxIter, param_config['maxIter'])
            if 'maxDepth' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.maxDepth, param_config['maxDepth'])
            if 'stepSize' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.stepSize, param_config['stepSize'])
            if 'subsamplingRate' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.subsamplingRate, param_config['subsamplingRate'])
        
        elif model_name == 'logistic_regression':
            if 'regParam' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.regParam, param_config['regParam'])
            if 'elasticNetParam' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.elasticNetParam, param_config['elasticNetParam'])
            if 'maxIter' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.maxIter, param_config['maxIter'])
        
        elif model_name == 'decision_tree':
            if 'maxDepth' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.maxDepth, param_config['maxDepth'])
            if 'minInstancesPerNode' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.minInstancesPerNode, param_config['minInstancesPerNode'])
            if 'minInfoGain' in param_config:
                param_grid_builder = param_grid_builder.addGrid(model.minInfoGain, param_config['minInfoGain'])
        
        return param_grid_builder.build()
    
    def _generate_random_params(self, model_name: str, num_combinations: int) -> List[Dict[str, Any]]:
        """Génère des combinaisons aléatoires de paramètres"""
        import random
        
        if model_name not in self.param_grids:
            return [{}]
        
        param_config = self.param_grids[model_name]
        combinations = []
        
        for _ in range(num_combinations):
            combination = {}
            for param_name, param_values in param_config.items():
                combination[param_name] = random.choice(param_values)
            combinations.append(combination)
        
        return combinations
    
    def _create_model_with_params(self, model_name: str, target_column: str, params: Dict[str, Any]):
        """Crée un modèle avec des paramètres spécifiques"""
        base_model = self._create_base_model(model_name, target_column)
        
        # Application des paramètres
        for param_name, param_value in params.items():
            if hasattr(base_model, param_name):
                setattr(base_model, param_name, param_value)
        
        return base_model
    
    def _extract_best_params(self, best_model, model_name: str) -> Dict[str, Any]:
        """Extrait les meilleurs paramètres d'un modèle entraîné"""
        best_params = {}
        
        if model_name not in self.param_grids:
            return best_params
        
        param_names = self.param_grids[model_name].keys()
        
        for param_name in param_names:
            if hasattr(best_model, param_name):
                param_value = getattr(best_model, param_name)
                best_params[param_name] = param_value
        
        return best_params
    
    def _get_default_params(self, model_name: str) -> Dict[str, Any]:
        """Retourne les paramètres par défaut d'un modèle"""
        default_params = {
            'random_forest': {
                'numTrees': 20,
                'maxDepth': 5,
                'minInstancesPerNode': 1,
                'subsamplingRate': 1.0
            },
            'gradient_boosting': {
                'maxIter': 20,
                'maxDepth': 5,
                'stepSize': 0.1,
                'subsamplingRate': 1.0
            },
            'logistic_regression': {
                'regParam': 0.0,
                'elasticNetParam': 0.0,
                'maxIter': 100
            },
            'decision_tree': {
                'maxDepth': 5,
                'minInstancesPerNode': 1,
                'minInfoGain': 0.0
            }
        }
        
        return default_params.get(model_name, {})
    
    def _calculate_std(self, values: List[float]) -> float:
        """Calcule l'écart-type d'une liste de valeurs"""
        if len(values) <= 1:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
    
    def get_tuning_summary(self) -> Dict[str, Any]:
        """Retourne un résumé des sessions de tuning"""
        return {
            'tuning_metrics': self.tuning_metrics,
            'supported_models': list(self.param_grids.keys()),
            'param_grids': self.param_grids,
            'tuning_methods': ['grid_search', 'random_search', 'cross_validation']
        }