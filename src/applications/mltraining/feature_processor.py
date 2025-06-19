"""
Composant FeatureProcessor pour la préparation des features ML depuis la couche Silver
"""

import time
from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, mean, stddev, min as spark_min, max as spark_max,
    count, sum as spark_sum, desc, asc, corr, abs as spark_abs
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, BooleanType
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, MinMaxScaler, StringIndexer, OneHotEncoder,
    ChiSqSelector, UnivariateFeatureSelector, VarianceThresholdSelector
)
from pyspark.ml.stat import Correlation
from pyspark.ml import Pipeline
import numpy as np

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import DataValidationError, MLModelError


class FeatureProcessor:
    """
    Composant de préparation des features pour ML depuis accidents_clean
    
    Fonctionnalités:
    - Scaling des variables numériques (StandardScaler, MinMaxScaler)
    - Encoding des variables catégorielles (OneHot, StringIndexer)
    - Feature selection basée sur importance et corrélations
    - Gestion des valeurs manquantes et outliers
    - Assemblage final des features pour ML
    """
    
    def __init__(self, config_manager: ConfigManager = None, spark: SparkSession = None):
        """Initialise le composant FeatureProcessor"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("feature_processor", self.config_manager)
        self.spark = spark
        
        # Configuration des features
        self.target_column = 'Severity'
        self.max_correlation_threshold = 0.95  # Seuil pour éliminer les features corrélées
        self.min_variance_threshold = 0.01     # Seuil de variance minimale
        self.max_missing_ratio = 0.30          # Ratio maximum de valeurs manquantes
        
        # Définition des colonnes par type
        self.numeric_features = [
            'Distance_mi', 'Temperature_F', 'Wind_Chill_F', 'Humidity_percent',
            'Pressure_in', 'Visibility_mi', 'Wind_Speed_mph', 'Precipitation_in',
            'Start_Lat', 'Start_Lng'
        ]
        
        self.categorical_features = [
            'Source', 'State', 'Weather_Condition', 'Sunrise_Sunset',
            'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight'
        ]
        
        self.boolean_features = [
            'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
            'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming',
            'Traffic_Signal', 'Turning_Loop'
        ]
        
        # Features temporelles créées par PREPROCESSOR
        self.temporal_features = [
            'accident_hour', 'accident_day_of_week', 'accident_month',
            'accident_season', 'is_weekend', 'is_rush_hour'
        ]
        
        # Features météorologiques enrichies
        self.weather_features = [
            'weather_category', 'weather_severity_score', 
            'visibility_category', 'temperature_category'
        ]
        
        # Features géographiques
        self.geo_features = [
            'distance_to_city_center', 'urban_rural_classification',
            'state_region', 'population_density_category'
        ]
        
        # Features infrastructure agrégées
        self.infra_features = [
            'infrastructure_count', 'safety_equipment_score', 'traffic_control_type'
        ]
        
        # Métriques de traitement
        self.processing_metrics = {
            'original_features': 0,
            'processed_features': 0,
            'selected_features': 0,
            'numeric_features_scaled': 0,
            'categorical_features_encoded': 0,
            'features_removed_correlation': 0,
            'features_removed_variance': 0,
            'features_removed_missing': 0,
            'outliers_handled': 0
        }
    
    def prepare_features_for_ml(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
        """Prépare toutes les features pour l'entraînement ML"""
        try:
            self.logger.info("Starting feature preparation for ML")
            start_time = time.time()
            
            # 1. Analyse initiale des données
            initial_analysis = self._analyze_initial_data(df)
            self.processing_metrics['original_features'] = len(df.columns) - 1  # -1 pour target
            
            # 2. Nettoyage des données
            cleaned_df = self._clean_data(df)
            
            # 3. Gestion des valeurs manquantes
            imputed_df = self._handle_missing_values(cleaned_df)
            
            # 4. Détection et traitement des outliers
            outlier_handled_df = self._handle_outliers(imputed_df)
            
            # 5. Scaling des features numériques
            scaled_df, numeric_scalers = self._scale_numeric_features(outlier_handled_df)
            
            # 6. Encoding des features catégorielles
            encoded_df, categorical_encoders = self._encode_categorical_features(scaled_df)
            
            # 7. Feature selection
            selected_df, selected_features = self._select_features(encoded_df)
            
            # 8. Assemblage final des features
            final_df, feature_vector_col = self._assemble_features(selected_df, selected_features)
            
            # 9. Validation finale
            self._validate_final_features(final_df, feature_vector_col)
            
            # Calcul du temps de traitement
            processing_time = time.time() - start_time
            self.processing_metrics['processing_time_seconds'] = processing_time
            
            # Informations sur les features finales
            feature_info = {
                'selected_features': selected_features,
                'feature_vector_column': feature_vector_col,
                'target_column': self.target_column,
                'numeric_scalers': numeric_scalers,
                'categorical_encoders': categorical_encoders,
                'processing_metrics': self.processing_metrics,
                'initial_analysis': initial_analysis
            }
            
            self.logger.log_performance(
                operation="feature_preparation",
                duration_seconds=processing_time,
                **self.processing_metrics
            )
            
            return final_df, feature_info
            
        except Exception as e:
            self.logger.error("Feature preparation failed", exception=e)
            raise MLModelError(
                message="Feature preparation for ML failed",
                operation="feature_preparation",
                metrics=self.processing_metrics
            )
    
    def _analyze_initial_data(self, df: DataFrame) -> Dict[str, Any]:
        """Analyse initiale des données"""
        try:
            self.logger.info("Analyzing initial data structure")
            
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Analyse des types de colonnes
            column_types = {}
            missing_counts = {}
            
            for column in df.columns:
                if column != self.target_column:
                    col_type = dict(df.dtypes)[column]
                    column_types[column] = col_type
                    
                    # Comptage des valeurs manquantes
                    missing_count = df.filter(col(column).isNull() | isnan(col(column))).count()
                    missing_counts[column] = {
                        'count': missing_count,
                        'ratio': missing_count / total_rows if total_rows > 0 else 0
                    }
            
            # Distribution de la variable cible
            target_distribution = df.groupBy(self.target_column).count().collect()
            target_dist_dict = {row[self.target_column]: row['count'] for row in target_distribution}
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': total_columns,
                'column_types': column_types,
                'missing_values': missing_counts,
                'target_distribution': target_dist_dict
            }
            
            self.logger.info("Initial data analysis completed",
                           total_rows=total_rows,
                           total_columns=total_columns,
                           target_classes=len(target_dist_dict))
            
            return analysis
            
        except Exception as e:
            self.logger.error("Initial data analysis failed", exception=e)
            raise DataValidationError(
                message="Initial data analysis failed",
                validation_type="data_analysis"
            )
    
    def _clean_data(self, df: DataFrame) -> DataFrame:
        """Nettoie les données de base"""
        try:
            self.logger.info("Cleaning data")
            
            # Suppression des colonnes avec trop de valeurs manquantes
            columns_to_remove = []
            for column in df.columns:
                if column != self.target_column:
                    missing_ratio = df.filter(col(column).isNull()).count() / df.count()
                    if missing_ratio > self.max_missing_ratio:
                        columns_to_remove.append(column)
            
            if columns_to_remove:
                df = df.drop(*columns_to_remove)
                self.processing_metrics['features_removed_missing'] = len(columns_to_remove)
                self.logger.info("Removed columns with high missing ratio",
                               columns_removed=columns_to_remove,
                               count=len(columns_to_remove))
            
            # Nettoyage des valeurs aberrantes dans les colonnes numériques
            for column in self.numeric_features:
                if column in df.columns:
                    # Remplacement des valeurs négatives inappropriées
                    if column in ['Distance_mi', 'Visibility_mi', 'Wind_Speed_mph']:
                        df = df.withColumn(column, when(col(column) < 0, 0).otherwise(col(column)))
                    
                    # Remplacement des valeurs extrêmes
                    if column == 'Temperature_F':
                        df = df.withColumn(column, 
                                         when(col(column) < -50, None)
                                         .when(col(column) > 150, None)
                                         .otherwise(col(column)))
            
            self.logger.info("Data cleaning completed")
            return df
            
        except Exception as e:
            self.logger.error("Data cleaning failed", exception=e)
            raise DataValidationError(
                message="Data cleaning failed",
                validation_type="data_cleaning"
            )
    
    def _handle_missing_values(self, df: DataFrame) -> DataFrame:
        """Gère les valeurs manquantes"""
        try:
            self.logger.info("Handling missing values")
            
            # Imputation des valeurs numériques par la médiane
            for column in self.numeric_features:
                if column in df.columns:
                    median_value = df.approxQuantile(column, [0.5], 0.01)[0]
                    if median_value is not None:
                        df = df.fillna({column: median_value})
            
            # Imputation des valeurs catégorielles par le mode
            for column in self.categorical_features:
                if column in df.columns:
                    mode_row = df.groupBy(column).count().orderBy(desc("count")).first()
                    if mode_row:
                        mode_value = mode_row[column]
                        df = df.fillna({column: mode_value})
            
            # Imputation des valeurs booléennes par False
            for column in self.boolean_features:
                if column in df.columns:
                    df = df.fillna({column: False})
            
            self.logger.info("Missing values handled")
            return df
            
        except Exception as e:
            self.logger.error("Missing values handling failed", exception=e)
            raise DataValidationError(
                message="Missing values handling failed",
                validation_type="missing_values"
            )
    
    def _handle_outliers(self, df: DataFrame) -> DataFrame:
        """Détecte et traite les outliers"""
        try:
            self.logger.info("Handling outliers")
            outliers_handled = 0
            
            for column in self.numeric_features:
                if column in df.columns:
                    # Calcul des quartiles et IQR
                    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
                    if len(quantiles) == 2:
                        q1, q3 = quantiles
                        iqr = q3 - q1
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr
                        
                        # Comptage des outliers
                        outlier_count = df.filter(
                            (col(column) < lower_bound) | (col(column) > upper_bound)
                        ).count()
                        
                        if outlier_count > 0:
                            # Winsorization: remplacement par les bornes
                            df = df.withColumn(column,
                                             when(col(column) < lower_bound, lower_bound)
                                             .when(col(column) > upper_bound, upper_bound)
                                             .otherwise(col(column)))
                            outliers_handled += outlier_count
            
            self.processing_metrics['outliers_handled'] = outliers_handled
            self.logger.info("Outliers handled", outliers_count=outliers_handled)
            return df
            
        except Exception as e:
            self.logger.error("Outlier handling failed", exception=e)
            raise DataValidationError(
                message="Outlier handling failed",
                validation_type="outlier_detection"
            )
    
    def _scale_numeric_features(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
        """Scale les features numériques"""
        try:
            self.logger.info("Scaling numeric features")
            scalers = {}
            scaled_columns = []
            
            for column in self.numeric_features:
                if column in df.columns:
                    # Assemblage en vecteur pour le scaling
                    assembler = VectorAssembler(inputCols=[column], outputCol=f"{column}_vector")
                    df = assembler.transform(df)
                    
                    # StandardScaler
                    scaler = StandardScaler(
                        inputCol=f"{column}_vector",
                        outputCol=f"{column}_scaled",
                        withStd=True,
                        withMean=True
                    )
                    
                    scaler_model = scaler.fit(df)
                    df = scaler_model.transform(df)
                    
                    # Extraction de la valeur scalée
                    from pyspark.sql.functions import udf
                    from pyspark.sql.types import DoubleType
                    
                    extract_udf = udf(lambda v: float(v[0]) if v else 0.0, DoubleType())
                    df = df.withColumn(f"{column}_scaled_value", extract_udf(col(f"{column}_scaled")))
                    
                    # Nettoyage des colonnes temporaires
                    df = df.drop(f"{column}_vector", f"{column}_scaled", column)
                    df = df.withColumnRenamed(f"{column}_scaled_value", column)
                    
                    scalers[column] = scaler_model
                    scaled_columns.append(column)
            
            self.processing_metrics['numeric_features_scaled'] = len(scaled_columns)
            self.logger.info("Numeric features scaled", columns=scaled_columns)
            
            return df, scalers
            
        except Exception as e:
            self.logger.error("Numeric feature scaling failed", exception=e)
            raise MLModelError(
                message="Numeric feature scaling failed",
                operation="feature_scaling"
            )
    
    def _encode_categorical_features(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
        """Encode les features catégorielles"""
        try:
            self.logger.info("Encoding categorical features")
            encoders = {}
            encoded_columns = []
            
            for column in self.categorical_features:
                if column in df.columns:
                    # StringIndexer pour convertir en indices
                    indexer = StringIndexer(
                        inputCol=column,
                        outputCol=f"{column}_indexed",
                        handleInvalid="keep"
                    )
                    
                    indexer_model = indexer.fit(df)
                    df = indexer_model.transform(df)
                    
                    # OneHotEncoder pour créer les variables dummy
                    encoder = OneHotEncoder(
                        inputCols=[f"{column}_indexed"],
                        outputCols=[f"{column}_encoded"],
                        handleInvalid="keep"
                    )
                    
                    encoder_model = encoder.fit(df)
                    df = encoder_model.transform(df)
                    
                    # Suppression des colonnes temporaires
                    df = df.drop(column, f"{column}_indexed")
                    
                    encoders[column] = {
                        'indexer': indexer_model,
                        'encoder': encoder_model
                    }
                    encoded_columns.append(f"{column}_encoded")
            
            self.processing_metrics['categorical_features_encoded'] = len(encoded_columns)
            self.logger.info("Categorical features encoded", columns=encoded_columns)
            
            return df, encoders
            
        except Exception as e:
            self.logger.error("Categorical feature encoding failed", exception=e)
            raise MLModelError(
                message="Categorical feature encoding failed",
                operation="feature_encoding"
            )
    
    def _select_features(self, df: DataFrame) -> Tuple[DataFrame, List[str]]:
        """Sélectionne les features les plus importantes"""
        try:
            self.logger.info("Selecting features")
            
            # Collecte de toutes les features disponibles
            all_features = []
            
            # Features numériques
            for column in self.numeric_features:
                if column in df.columns:
                    all_features.append(column)
            
            # Features booléennes
            for column in self.boolean_features:
                if column in df.columns:
                    all_features.append(column)
            
            # Features temporelles
            for column in self.temporal_features:
                if column in df.columns:
                    all_features.append(column)
            
            # Features météorologiques
            for column in self.weather_features:
                if column in df.columns:
                    all_features.append(column)
            
            # Features géographiques
            for column in self.geo_features:
                if column in df.columns:
                    all_features.append(column)
            
            # Features infrastructure
            for column in self.infra_features:
                if column in df.columns:
                    all_features.append(column)
            
            # Features catégorielles encodées
            for column in df.columns:
                if column.endswith('_encoded'):
                    all_features.append(column)
            
            # Suppression des features avec variance trop faible
            selected_features = self._remove_low_variance_features(df, all_features)
            
            # Suppression des features trop corrélées
            selected_features = self._remove_correlated_features(df, selected_features)
            
            self.processing_metrics['selected_features'] = len(selected_features)
            self.logger.info("Feature selection completed",
                           original_count=len(all_features),
                           selected_count=len(selected_features))
            
            return df, selected_features
            
        except Exception as e:
            self.logger.error("Feature selection failed", exception=e)
            raise MLModelError(
                message="Feature selection failed",
                operation="feature_selection"
            )
    
    def _remove_low_variance_features(self, df: DataFrame, features: List[str]) -> List[str]:
        """Supprime les features avec variance trop faible"""
        try:
            selected_features = []
            removed_count = 0
            
            for feature in features:
                if feature in df.columns and not feature.endswith('_encoded'):
                    # Calcul de la variance
                    variance = df.select(feature).rdd.map(lambda x: x[0]).variance()
                    if variance >= self.min_variance_threshold:
                        selected_features.append(feature)
                    else:
                        removed_count += 1
                else:
                    # Garder les features encodées (vecteurs)
                    selected_features.append(feature)
            
            self.processing_metrics['features_removed_variance'] = removed_count
            return selected_features
            
        except Exception as e:
            self.logger.error("Low variance feature removal failed", exception=e)
            return features
    
    def _remove_correlated_features(self, df: DataFrame, features: List[str]) -> List[str]:
        """Supprime les features trop corrélées"""
        try:
            # Pour simplifier, on garde toutes les features pour cette version
            # Une implémentation complète nécessiterait le calcul de la matrice de corrélation
            # et la suppression itérative des features corrélées
            
            self.processing_metrics['features_removed_correlation'] = 0
            return features
            
        except Exception as e:
            self.logger.error("Correlated feature removal failed", exception=e)
            return features
    
    def _assemble_features(self, df: DataFrame, selected_features: List[str]) -> Tuple[DataFrame, str]:
        """Assemble toutes les features en un vecteur final"""
        try:
            self.logger.info("Assembling final feature vector")
            
            feature_vector_col = "features"
            
            # VectorAssembler pour créer le vecteur de features
            assembler = VectorAssembler(
                inputCols=selected_features,
                outputCol=feature_vector_col,
                handleInvalid="keep"
            )
            
            final_df = assembler.transform(df)
            
            # Garder seulement les colonnes nécessaires
            final_df = final_df.select(feature_vector_col, self.target_column)
            
            self.logger.info("Feature vector assembled",
                           feature_count=len(selected_features),
                           vector_column=feature_vector_col)
            
            return final_df, feature_vector_col
            
        except Exception as e:
            self.logger.error("Feature assembly failed", exception=e)
            raise MLModelError(
                message="Feature assembly failed",
                operation="feature_assembly"
            )
    
    def _validate_final_features(self, df: DataFrame, feature_vector_col: str) -> None:
        """Valide les features finales"""
        try:
            self.logger.info("Validating final features")
            
            # Vérification de la présence des colonnes requises
            required_columns = [feature_vector_col, self.target_column]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise DataValidationError(
                    message=f"Missing required columns: {missing_columns}",
                    validation_type="final_validation"
                )
            
            # Vérification des valeurs nulles dans le vecteur de features
            null_count = df.filter(col(feature_vector_col).isNull()).count()
            if null_count > 0:
                self.logger.warning("Found null values in feature vector",
                                  null_count=null_count)
            
            # Vérification de la distribution de la variable cible
            target_counts = df.groupBy(self.target_column).count().collect()
            if len(target_counts) < 2:
                raise DataValidationError(
                    message="Target variable has less than 2 classes",
                    validation_type="target_validation"
                )
            
            self.logger.info("Final feature validation completed successfully")
            
        except Exception as e:
            self.logger.error("Final feature validation failed", exception=e)
            raise DataValidationError(
                message="Final feature validation failed",
                validation_type="final_validation"
            )