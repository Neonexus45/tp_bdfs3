"""
Composant ModelTrainer pour l'entraînement de multiples modèles ML
"""

import time
from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.classification import (
    RandomForestClassifier, GBTClassifier, LogisticRegression, DecisionTreeClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import MLModelError, SparkJobError


class ModelTrainer:
    """
    Composant d'entraînement de multiples modèles ML pour classification de sévérité
    
    Modèles supportés:
    - RandomForestClassifier: Robuste, interprétable
    - GradientBoostingClassifier (GBT): Performance élevée
    - LogisticRegression: Baseline simple et rapide
    - DecisionTreeClassifier: Natif Spark, interprétable
    
    Fonctionnalités:
    - Configuration optimisée pour classification multi-classe (sévérité 1-4)
    - Gestion du déséquilibre des classes
    - Cross-validation intégrée
    - Métriques de performance détaillées
    """
    
    def __init__(self, config_manager: ConfigManager = None, spark: SparkSession = None):
        """Initialise le composant ModelTrainer"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("model_trainer", self.config_manager)
        self.spark = spark
        
        # Configuration des modèles
        self.random_seed = 42
        self.cv_folds = 3  # Cross-validation folds
        
        # Paramètres par défaut pour chaque modèle
        self.default_params = {
            'random_forest': {
                'numTrees': 100,
                'maxDepth': 15,
                'minInstancesPerNode': 1,
                'subsamplingRate': 0.8,
                'featureSubsetStrategy': 'auto',
                'seed': self.random_seed
            },
            'gradient_boosting': {
                'maxIter': 100,
                'maxDepth': 10,
                'stepSize': 0.1,
                'subsamplingRate': 0.8,
                'seed': self.random_seed
            },
            'logistic_regression': {
                'maxIter': 100,
                'regParam': 0.1,
                'elasticNetParam': 0.0,
                'family': 'multinomial',
                'seed': self.random_seed
            },
            'decision_tree': {
                'maxDepth': 15,
                'minInstancesPerNode': 1,
                'minInfoGain': 0.0,
                'seed': self.random_seed
            }
        }
        
        # Métriques d'entraînement
        self.training_metrics = {
            'models_trained': 0,
            'total_training_time': 0,
            'best_model': None,
            'best_score': 0.0
        }
    
    def train_multiple_models(self, train_df: DataFrame, validation_df: DataFrame,
                            feature_columns: List[str], target_column: str) -> Dict[str, Any]:
        """Entraîne plusieurs modèles et retourne leurs performances"""
        try:
            self.logger.info("Starting training of multiple ML models",
                           models=['random_forest', 'gradient_boosting', 'logistic_regression', 'decision_tree'])
            
            start_time = time.time()
            results = {}
            
            # 1. Random Forest
            self.logger.info("Training Random Forest model")
            rf_result = self._train_random_forest(train_df, validation_df, feature_columns, target_column)
            results['random_forest'] = rf_result
            
            # 2. Gradient Boosting
            self.logger.info("Training Gradient Boosting model")
            gb_result = self._train_gradient_boosting(train_df, validation_df, feature_columns, target_column)
            results['gradient_boosting'] = gb_result
            
            # 3. Logistic Regression
            self.logger.info("Training Logistic Regression model")
            lr_result = self._train_logistic_regression(train_df, validation_df, feature_columns, target_column)
            results['logistic_regression'] = lr_result
            
            # 4. Decision Tree
            self.logger.info("Training Decision Tree model")
            dt_result = self._train_decision_tree(train_df, validation_df, feature_columns, target_column)
            results['decision_tree'] = dt_result
            
            # Calcul du temps total
            total_time = time.time() - start_time
            self.training_metrics['total_training_time'] = total_time
            self.training_metrics['models_trained'] = len(results)
            
            # Identification du meilleur modèle
            best_model_name = max(results.keys(), 
                                key=lambda k: results[k]['validation_metrics']['f1_score_macro'])
            self.training_metrics['best_model'] = best_model_name
            self.training_metrics['best_score'] = results[best_model_name]['validation_metrics']['f1_score_macro']
            
            self.logger.log_performance(
                operation="multiple_models_training",
                duration_seconds=total_time,
                models_count=len(results),
                best_model=best_model_name,
                best_f1_score=self.training_metrics['best_score']
            )
            
            return results
            
        except Exception as e:
            self.logger.error("Multiple models training failed", exception=e)
            raise MLModelError(
                message="Multiple models training failed",
                operation="train_multiple_models",
                metrics=self.training_metrics
            )
    
    def _train_random_forest(self, train_df: DataFrame, validation_df: DataFrame,
                           feature_columns: List[str], target_column: str) -> Dict[str, Any]:
        """Entraîne un modèle Random Forest"""
        try:
            start_time = time.time()
            
            # Configuration du modèle
            rf = RandomForestClassifier(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                probabilityCol="probability",
                **self.default_params['random_forest']
            )
            
            # Entraînement
            rf_model = rf.fit(train_df)
            
            # Prédictions sur validation
            validation_predictions = rf_model.transform(validation_df)
            
            # Calcul des métriques
            validation_metrics = self._calculate_metrics(validation_predictions, target_column)
            
            # Feature importance
            feature_importance = self._get_feature_importance(rf_model, feature_columns)
            
            training_time = time.time() - start_time
            
            result = {
                'model': rf_model,
                'model_type': 'RandomForestClassifier',
                'training_time_seconds': training_time,
                'validation_metrics': validation_metrics,
                'feature_importance': feature_importance,
                'model_params': self.default_params['random_forest']
            }
            
            self.logger.info("Random Forest training completed",
                           training_time=training_time,
                           accuracy=validation_metrics['accuracy'],
                           f1_score=validation_metrics['f1_score_macro'])
            
            return result
            
        except Exception as e:
            self.logger.error("Random Forest training failed", exception=e)
            raise MLModelError(
                message="Random Forest training failed",
                model_name="random_forest",
                operation="training"
            )
    
    def _train_gradient_boosting(self, train_df: DataFrame, validation_df: DataFrame,
                               feature_columns: List[str], target_column: str) -> Dict[str, Any]:
        """Entraîne un modèle Gradient Boosting"""
        try:
            start_time = time.time()
            
            # Configuration du modèle
            gbt = GBTClassifier(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                **self.default_params['gradient_boosting']
            )
            
            # Entraînement
            gbt_model = gbt.fit(train_df)
            
            # Prédictions sur validation
            validation_predictions = gbt_model.transform(validation_df)
            
            # Calcul des métriques
            validation_metrics = self._calculate_metrics(validation_predictions, target_column)
            
            # Feature importance
            feature_importance = self._get_feature_importance(gbt_model, feature_columns)
            
            training_time = time.time() - start_time
            
            result = {
                'model': gbt_model,
                'model_type': 'GBTClassifier',
                'training_time_seconds': training_time,
                'validation_metrics': validation_metrics,
                'feature_importance': feature_importance,
                'model_params': self.default_params['gradient_boosting']
            }
            
            self.logger.info("Gradient Boosting training completed",
                           training_time=training_time,
                           accuracy=validation_metrics['accuracy'],
                           f1_score=validation_metrics['f1_score_macro'])
            
            return result
            
        except Exception as e:
            self.logger.error("Gradient Boosting training failed", exception=e)
            raise MLModelError(
                message="Gradient Boosting training failed",
                model_name="gradient_boosting",
                operation="training"
            )
    
    def _train_logistic_regression(self, train_df: DataFrame, validation_df: DataFrame,
                                 feature_columns: List[str], target_column: str) -> Dict[str, Any]:
        """Entraîne un modèle Logistic Regression"""
        try:
            start_time = time.time()
            
            # Configuration du modèle
            lr = LogisticRegression(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                probabilityCol="probability",
                **self.default_params['logistic_regression']
            )
            
            # Entraînement
            lr_model = lr.fit(train_df)
            
            # Prédictions sur validation
            validation_predictions = lr_model.transform(validation_df)
            
            # Calcul des métriques
            validation_metrics = self._calculate_metrics(validation_predictions, target_column)
            
            training_time = time.time() - start_time
            
            result = {
                'model': lr_model,
                'model_type': 'LogisticRegression',
                'training_time_seconds': training_time,
                'validation_metrics': validation_metrics,
                'feature_importance': {},  # LR n'a pas de feature importance directe
                'model_params': self.default_params['logistic_regression']
            }
            
            self.logger.info("Logistic Regression training completed",
                           training_time=training_time,
                           accuracy=validation_metrics['accuracy'],
                           f1_score=validation_metrics['f1_score_macro'])
            
            return result
            
        except Exception as e:
            self.logger.error("Logistic Regression training failed", exception=e)
            raise MLModelError(
                message="Logistic Regression training failed",
                model_name="logistic_regression",
                operation="training"
            )
    
    def _train_decision_tree(self, train_df: DataFrame, validation_df: DataFrame,
                           feature_columns: List[str], target_column: str) -> Dict[str, Any]:
        """Entraîne un modèle Decision Tree"""
        try:
            start_time = time.time()
            
            # Configuration du modèle
            dt = DecisionTreeClassifier(
                featuresCol="features",
                labelCol=target_column,
                predictionCol="prediction",
                probabilityCol="probability",
                **self.default_params['decision_tree']
            )
            
            # Entraînement
            dt_model = dt.fit(train_df)
            
            # Prédictions sur validation
            validation_predictions = dt_model.transform(validation_df)
            
            # Calcul des métriques
            validation_metrics = self._calculate_metrics(validation_predictions, target_column)
            
            # Feature importance
            feature_importance = self._get_feature_importance(dt_model, feature_columns)
            
            training_time = time.time() - start_time
            
            result = {
                'model': dt_model,
                'model_type': 'DecisionTreeClassifier',
                'training_time_seconds': training_time,
                'validation_metrics': validation_metrics,
                'feature_importance': feature_importance,
                'model_params': self.default_params['decision_tree']
            }
            
            self.logger.info("Decision Tree training completed",
                           training_time=training_time,
                           accuracy=validation_metrics['accuracy'],
                           f1_score=validation_metrics['f1_score_macro'])
            
            return result
            
        except Exception as e:
            self.logger.error("Decision Tree training failed", exception=e)
            raise MLModelError(
                message="Decision Tree training failed",
                model_name="decision_tree",
                operation="training"
            )
    
    def _calculate_metrics(self, predictions_df: DataFrame, target_column: str) -> Dict[str, float]:
        """Calcule les métriques de performance"""
        try:
            # Evaluateurs pour différentes métriques
            accuracy_evaluator = MulticlassClassificationEvaluator(
                labelCol=target_column,
                predictionCol="prediction",
                metricName="accuracy"
            )
            
            f1_evaluator = MulticlassClassificationEvaluator(
                labelCol=target_column,
                predictionCol="prediction",
                metricName="f1"
            )
            
            precision_evaluator = MulticlassClassificationEvaluator(
                labelCol=target_column,
                predictionCol="prediction",
                metricName="weightedPrecision"
            )
            
            recall_evaluator = MulticlassClassificationEvaluator(
                labelCol=target_column,
                predictionCol="prediction",
                metricName="weightedRecall"
            )
            
            # Calcul des métriques
            accuracy = accuracy_evaluator.evaluate(predictions_df)
            f1_score_macro = f1_evaluator.evaluate(predictions_df)
            precision_macro = precision_evaluator.evaluate(predictions_df)
            recall_macro = recall_evaluator.evaluate(predictions_df)
            
            metrics = {
                'accuracy': round(accuracy, 4),
                'f1_score_macro': round(f1_score_macro, 4),
                'precision_macro': round(precision_macro, 4),
                'recall_macro': round(recall_macro, 4)
            }
            
            return metrics
            
        except Exception as e:
            self.logger.error("Metrics calculation failed", exception=e)
            return {
                'accuracy': 0.0,
                'f1_score_macro': 0.0,
                'precision_macro': 0.0,
                'recall_macro': 0.0
            }
    
    def _get_feature_importance(self, model, feature_columns: List[str]) -> Dict[str, float]:
        """Extrait l'importance des features du modèle"""
        try:
            if hasattr(model, 'featureImportances'):
                importances = model.featureImportances.toArray()
                
                # Création du dictionnaire feature -> importance
                feature_importance = {}
                for i, importance in enumerate(importances):
                    if i < len(feature_columns):
                        feature_importance[feature_columns[i]] = round(float(importance), 4)
                
                # Tri par importance décroissante
                sorted_importance = dict(sorted(feature_importance.items(), 
                                              key=lambda x: x[1], reverse=True))
                
                return sorted_importance
            else:
                return {}
                
        except Exception as e:
            self.logger.error("Feature importance extraction failed", exception=e)
            return {}
    
    def train_single_model(self, model_name: str, train_df: DataFrame, validation_df: DataFrame,
                          feature_columns: List[str], target_column: str,
                          custom_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Entraîne un seul modèle avec des paramètres personnalisés"""
        try:
            self.logger.info("Training single model", model_name=model_name)
            
            # Mise à jour des paramètres si fournis
            if custom_params:
                params = {**self.default_params.get(model_name, {}), **custom_params}
            else:
                params = self.default_params.get(model_name, {})
            
            # Entraînement selon le type de modèle
            if model_name == 'random_forest':
                # Mise à jour temporaire des paramètres
                original_params = self.default_params['random_forest'].copy()
                self.default_params['random_forest'].update(params)
                result = self._train_random_forest(train_df, validation_df, feature_columns, target_column)
                self.default_params['random_forest'] = original_params
                
            elif model_name == 'gradient_boosting':
                original_params = self.default_params['gradient_boosting'].copy()
                self.default_params['gradient_boosting'].update(params)
                result = self._train_gradient_boosting(train_df, validation_df, feature_columns, target_column)
                self.default_params['gradient_boosting'] = original_params
                
            elif model_name == 'logistic_regression':
                original_params = self.default_params['logistic_regression'].copy()
                self.default_params['logistic_regression'].update(params)
                result = self._train_logistic_regression(train_df, validation_df, feature_columns, target_column)
                self.default_params['logistic_regression'] = original_params
                
            elif model_name == 'decision_tree':
                original_params = self.default_params['decision_tree'].copy()
                self.default_params['decision_tree'].update(params)
                result = self._train_decision_tree(train_df, validation_df, feature_columns, target_column)
                self.default_params['decision_tree'] = original_params
                
            else:
                raise MLModelError(
                    message=f"Unknown model name: {model_name}",
                    model_name=model_name,
                    operation="single_model_training"
                )
            
            return result
            
        except Exception as e:
            self.logger.error("Single model training failed", exception=e, model_name=model_name)
            raise MLModelError(
                message=f"Single model training failed for {model_name}",
                model_name=model_name,
                operation="single_model_training"
            )
    
    def get_training_summary(self) -> Dict[str, Any]:
        """Retourne un résumé de l'entraînement"""
        return {
            'training_metrics': self.training_metrics,
            'supported_models': list(self.default_params.keys()),
            'default_parameters': self.default_params
        }