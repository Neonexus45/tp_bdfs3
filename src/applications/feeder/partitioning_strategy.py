"""
Composant PartitioningStrategy pour le partitioning intelligent des données US-Accidents
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, date_format, when, lit, 
    current_date, to_date, coalesce
)

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.utils.spark_utils import SparkUtils
from ...common.exceptions.custom_exceptions import DataValidationError, SparkJobError


class PartitioningStrategy:
    """
    Stratégie de partitioning intelligent pour les données US-Accidents
    
    Fonctionnalités:
    - Partitioning par date d'ingestion (YYYY/MM/DD)
    - Sous-partitioning par état (State column)
    - Optimisation nombre de partitions (éviter small files)
    - Coalescing intelligent avant écriture
    - Support partitioning hiérarchique
    - Calcul automatique des partitions optimales
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise la stratégie de partitioning"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("partitioning_strategy", self.config_manager)
        self.spark_utils = SparkUtils(self.config_manager)
        
        # Configuration du partitioning
        self.partition_config = {
            'primary_columns': self.config_manager.get('data.partition_columns', ['date', 'state']),
            'target_partition_size_mb': 128,  # Taille cible par partition
            'min_partition_size_mb': 64,      # Taille minimale acceptable
            'max_partitions_per_state': 50,   # Maximum de partitions par état
            'enable_dynamic_partitioning': True,
            'coalesce_small_partitions': True
        }
        
        # États américains avec estimation du volume de données
        self.state_volume_estimates = {
            'CA': 'high',    # Californie - volume élevé
            'TX': 'high',    # Texas - volume élevé
            'FL': 'high',    # Floride - volume élevé
            'NY': 'medium',  # New York - volume moyen
            'PA': 'medium',  # Pennsylvanie - volume moyen
            'IL': 'medium',  # Illinois - volume moyen
            'OH': 'medium',  # Ohio - volume moyen
            'GA': 'medium',  # Géorgie - volume moyen
            'NC': 'medium',  # Caroline du Nord - volume moyen
            'MI': 'medium'   # Michigan - volume moyen
        }
        
        # Stratégies de partitioning par volume
        self.partitioning_strategies = {
            'high': {'partitions_per_day': 4, 'coalesce_factor': 2},
            'medium': {'partitions_per_day': 2, 'coalesce_factor': 3},
            'low': {'partitions_per_day': 1, 'coalesce_factor': 5}
        }
        
        # Métriques de partitioning
        self.partitioning_metrics = {
            'original_partitions': 0,
            'final_partitions': 0,
            'partitions_by_state': {},
            'partitions_by_date': {},
            'coalescing_applied': False,
            'optimization_applied': False
        }
    
    def apply_partitioning(self, df: DataFrame, strategy: str = 'auto') -> DataFrame:
        """
        Applique la stratégie de partitioning au DataFrame
        
        Args:
            df: DataFrame à partitionner
            strategy: Stratégie à appliquer ('auto', 'date_only', 'state_only', 'hierarchical')
            
        Returns:
            DataFrame partitionné et optimisé
        """
        try:
            self.logger.info("Applying partitioning strategy", 
                           strategy=strategy,
                           original_partitions=df.rdd.getNumPartitions(),
                           total_rows=df.count())
            
            # Préparation des colonnes de partitioning
            prepared_df = self._prepare_partitioning_columns(df)
            
            # Application de la stratégie sélectionnée
            if strategy == 'auto':
                partitioned_df = self._apply_auto_partitioning(prepared_df)
            elif strategy == 'date_only':
                partitioned_df = self._apply_date_partitioning(prepared_df)
            elif strategy == 'state_only':
                partitioned_df = self._apply_state_partitioning(prepared_df)
            elif strategy == 'hierarchical':
                partitioned_df = self._apply_hierarchical_partitioning(prepared_df)
            else:
                self.logger.warning(f"Unknown strategy '{strategy}', using auto")
                partitioned_df = self._apply_auto_partitioning(prepared_df)
            
            # Optimisation post-partitioning
            optimized_df = self._optimize_partitions(partitioned_df)
            
            # Calcul des métriques finales
            self._calculate_partitioning_metrics(df, optimized_df)
            
            # Logging des résultats
            self._log_partitioning_results()
            
            return optimized_df
            
        except Exception as e:
            self.logger.error("Partitioning strategy failed", exception=e)
            raise SparkJobError(
                message=f"Partitioning failed: {str(e)}",
                job_name="partitioning_strategy",
                stage="apply_partitioning"
            )
    
    def _prepare_partitioning_columns(self, df: DataFrame) -> DataFrame:
        """Prépare les colonnes nécessaires au partitioning"""
        try:
            prepared_df = df
            
            # Ajout de la colonne de date de partitioning si pas présente
            if 'partition_date' not in df.columns:
                if 'ingestion_date' in df.columns:
                    prepared_df = prepared_df.withColumn('partition_date', col('ingestion_date'))
                elif 'Start_Time' in df.columns:
                    prepared_df = prepared_df.withColumn('partition_date', to_date(col('Start_Time')))
                else:
                    prepared_df = prepared_df.withColumn('partition_date', current_date())
            
            # Ajout des colonnes de partitioning hiérarchique
            prepared_df = prepared_df.withColumn('partition_year', year(col('partition_date')))
            prepared_df = prepared_df.withColumn('partition_month', month(col('partition_date')))
            prepared_df = prepared_df.withColumn('partition_day', dayofmonth(col('partition_date')))
            
            # Normalisation de la colonne State
            if 'State' in df.columns:
                prepared_df = prepared_df.withColumn(
                    'partition_state', 
                    when(col('State').isNull(), lit('UNKNOWN')).otherwise(col('State'))
                )
            else:
                prepared_df = prepared_df.withColumn('partition_state', lit('UNKNOWN'))
            
            self.logger.info("Partitioning columns prepared", 
                           columns_added=['partition_date', 'partition_year', 'partition_month', 'partition_day', 'partition_state'])
            
            return prepared_df
            
        except Exception as e:
            self.logger.error("Failed to prepare partitioning columns", exception=e)
            return df
    
    def _apply_auto_partitioning(self, df: DataFrame) -> DataFrame:
        """Applique une stratégie de partitioning automatique basée sur les données"""
        try:
            # Analyse des données pour déterminer la meilleure stratégie
            data_analysis = self._analyze_data_distribution(df)
            
            # Sélection de la stratégie basée sur l'analyse
            if data_analysis['unique_dates'] > 30 and data_analysis['unique_states'] > 10:
                # Beaucoup de dates et d'états -> partitioning hiérarchique
                return self._apply_hierarchical_partitioning(df)
            elif data_analysis['unique_dates'] > data_analysis['unique_states']:
                # Plus de dates que d'états -> partitioning par date
                return self._apply_date_partitioning(df)
            else:
                # Plus d'états que de dates -> partitioning par état
                return self._apply_state_partitioning(df)
                
        except Exception as e:
            self.logger.error("Auto partitioning failed", exception=e)
            return df
    
    def _apply_date_partitioning(self, df: DataFrame) -> DataFrame:
        """Applique un partitioning par date"""
        try:
            # Partitioning par année/mois/jour
            partitioned_df = df.repartition(
                col('partition_year'),
                col('partition_month'),
                col('partition_day')
            )
            
            self.logger.info("Date partitioning applied", 
                           partition_columns=['partition_year', 'partition_month', 'partition_day'])
            
            return partitioned_df
            
        except Exception as e:
            self.logger.error("Date partitioning failed", exception=e)
            return df
    
    def _apply_state_partitioning(self, df: DataFrame) -> DataFrame:
        """Applique un partitioning par état"""
        try:
            # Calcul du nombre optimal de partitions par état
            state_distribution = self._get_state_distribution(df)
            optimal_partitions = self._calculate_optimal_partitions_by_state(state_distribution)
            
            # Partitioning avec nombre optimal de partitions
            partitioned_df = df.repartition(optimal_partitions, col('partition_state'))
            
            self.logger.info("State partitioning applied", 
                           partition_columns=['partition_state'],
                           optimal_partitions=optimal_partitions)
            
            return partitioned_df
            
        except Exception as e:
            self.logger.error("State partitioning failed", exception=e)
            return df
    
    def _apply_hierarchical_partitioning(self, df: DataFrame) -> DataFrame:
        """Applique un partitioning hiérarchique (date + état)"""
        try:
            # Partitioning hiérarchique: date puis état
            partitioned_df = df.repartition(
                col('partition_year'),
                col('partition_month'),
                col('partition_state')
            )
            
            self.logger.info("Hierarchical partitioning applied", 
                           partition_columns=['partition_year', 'partition_month', 'partition_state'])
            
            return partitioned_df
            
        except Exception as e:
            self.logger.error("Hierarchical partitioning failed", exception=e)
            return df
    
    def _analyze_data_distribution(self, df: DataFrame) -> Dict[str, Any]:
        """Analyse la distribution des données pour optimiser le partitioning"""
        try:
            # Analyse des dates uniques
            unique_dates = df.select('partition_date').distinct().count()
            
            # Analyse des états uniques
            unique_states = df.select('partition_state').distinct().count()
            
            # Distribution par état
            state_distribution = df.groupBy('partition_state').count().collect()
            state_counts = {row['partition_state']: row['count'] for row in state_distribution}
            
            # Distribution par date
            date_distribution = df.groupBy('partition_date').count().collect()
            date_counts = {str(row['partition_date']): row['count'] for row in date_distribution}
            
            # Calcul des statistiques
            total_rows = df.count()
            avg_rows_per_state = total_rows / unique_states if unique_states > 0 else 0
            avg_rows_per_date = total_rows / unique_dates if unique_dates > 0 else 0
            
            analysis = {
                'total_rows': total_rows,
                'unique_dates': unique_dates,
                'unique_states': unique_states,
                'avg_rows_per_state': avg_rows_per_state,
                'avg_rows_per_date': avg_rows_per_date,
                'state_distribution': state_counts,
                'date_distribution': date_counts,
                'recommended_strategy': self._recommend_partitioning_strategy(
                    unique_dates, unique_states, avg_rows_per_state, avg_rows_per_date
                )
            }
            
            self.logger.info("Data distribution analyzed", **{
                k: v for k, v in analysis.items() 
                if k not in ['state_distribution', 'date_distribution']
            })
            
            return analysis
            
        except Exception as e:
            self.logger.error("Data distribution analysis failed", exception=e)
            return {
                'total_rows': df.count(),
                'unique_dates': 1,
                'unique_states': 1,
                'recommended_strategy': 'simple'
            }
    
    def _recommend_partitioning_strategy(self, unique_dates: int, unique_states: int, 
                                       avg_rows_per_state: float, avg_rows_per_date: float) -> str:
        """Recommande une stratégie de partitioning basée sur l'analyse"""
        try:
            # Critères de décision
            if unique_dates > 100 and unique_states > 20:
                return 'hierarchical'
            elif avg_rows_per_date > avg_rows_per_state and unique_dates > 10:
                return 'date_primary'
            elif avg_rows_per_state > avg_rows_per_date and unique_states > 5:
                return 'state_primary'
            else:
                return 'simple'
                
        except Exception:
            return 'simple'
    
    def _get_state_distribution(self, df: DataFrame) -> Dict[str, int]:
        """Obtient la distribution des enregistrements par état"""
        try:
            state_counts = df.groupBy('partition_state').count().collect()
            return {row['partition_state']: row['count'] for row in state_counts}
            
        except Exception as e:
            self.logger.error("Failed to get state distribution", exception=e)
            return {}
    
    def _calculate_optimal_partitions_by_state(self, state_distribution: Dict[str, int]) -> int:
        """Calcule le nombre optimal de partitions basé sur la distribution par état"""
        try:
            if not state_distribution:
                return 200  # Valeur par défaut
            
            total_rows = sum(state_distribution.values())
            target_rows_per_partition = (self.partition_config['target_partition_size_mb'] * 1024 * 1024) / 1000  # Estimation
            
            optimal_partitions = max(1, int(total_rows / target_rows_per_partition))
            
            # Limitation basée sur le nombre d'états
            max_partitions = len(state_distribution) * self.partition_config['max_partitions_per_state']
            optimal_partitions = min(optimal_partitions, max_partitions)
            
            return optimal_partitions
            
        except Exception as e:
            self.logger.error("Failed to calculate optimal partitions", exception=e)
            return 200
    
    def _optimize_partitions(self, df: DataFrame) -> DataFrame:
        """Optimise les partitions après partitioning"""
        try:
            current_partitions = df.rdd.getNumPartitions()
            
            # Estimation de la taille des données
            estimated_size_mb = self.spark_utils.estimate_dataframe_size_mb(df)
            
            # Calcul du nombre optimal de partitions
            optimal_partitions = max(1, int(estimated_size_mb / self.partition_config['target_partition_size_mb']))
            
            optimized_df = df
            
            # Coalescing si nécessaire
            if (self.partition_config['coalesce_small_partitions'] and 
                current_partitions > optimal_partitions * 2):
                
                optimized_df = df.coalesce(optimal_partitions)
                self.partitioning_metrics['coalescing_applied'] = True
                
                self.logger.info("Partition coalescing applied", 
                               original_partitions=current_partitions,
                               coalesced_partitions=optimal_partitions)
            
            # Repartitioning si les partitions sont trop petites
            elif current_partitions < optimal_partitions / 2:
                optimized_df = df.repartition(optimal_partitions)
                self.partitioning_metrics['optimization_applied'] = True
                
                self.logger.info("Partition repartitioning applied", 
                               original_partitions=current_partitions,
                               repartitioned_partitions=optimal_partitions)
            
            return optimized_df
            
        except Exception as e:
            self.logger.error("Partition optimization failed", exception=e)
            return df
    
    def _calculate_partitioning_metrics(self, original_df: DataFrame, final_df: DataFrame):
        """Calcule les métriques de partitioning"""
        try:
            self.partitioning_metrics.update({
                'original_partitions': original_df.rdd.getNumPartitions(),
                'final_partitions': final_df.rdd.getNumPartitions(),
                'partitions_by_state': self._count_partitions_by_column(final_df, 'partition_state'),
                'partitions_by_date': self._count_partitions_by_column(final_df, 'partition_date'),
                'estimated_size_mb': self.spark_utils.estimate_dataframe_size_mb(final_df)
            })
            
        except Exception as e:
            self.logger.error("Failed to calculate partitioning metrics", exception=e)
    
    def _count_partitions_by_column(self, df: DataFrame, column: str) -> Dict[str, int]:
        """Compte les partitions par valeur de colonne"""
        try:
            if column in df.columns:
                counts = df.groupBy(column).count().collect()
                return {str(row[column]): row['count'] for row in counts}
            return {}
            
        except Exception as e:
            self.logger.error(f"Failed to count partitions by {column}", exception=e)
            return {}
    
    def _log_partitioning_results(self):
        """Log les résultats du partitioning"""
        try:
            self.logger.log_performance(
                operation="data_partitioning",
                duration_seconds=0,
                original_partitions=self.partitioning_metrics['original_partitions'],
                final_partitions=self.partitioning_metrics['final_partitions'],
                coalescing_applied=self.partitioning_metrics['coalescing_applied'],
                optimization_applied=self.partitioning_metrics['optimization_applied'],
                estimated_size_mb=self.partitioning_metrics.get('estimated_size_mb', 0)
            )
            
        except Exception as e:
            self.logger.error("Failed to log partitioning results", exception=e)
    
    def get_partitioning_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques de partitioning"""
        return self.partitioning_metrics.copy()
    
    def set_partition_config(self, config: Dict[str, Any]):
        """Met à jour la configuration de partitioning"""
        self.partition_config.update(config)
        self.logger.info("Partition configuration updated", **config)
    
    def estimate_partition_count(self, df: DataFrame, strategy: str = 'auto') -> Dict[str, Any]:
        """
        Estime le nombre de partitions qui seront créées
        
        Args:
            df: DataFrame à analyser
            strategy: Stratégie de partitioning
            
        Returns:
            Dict avec les estimations
        """
        try:
            analysis = self._analyze_data_distribution(df)
            estimated_size_mb = self.spark_utils.estimate_dataframe_size_mb(df)
            
            estimates = {
                'current_partitions': df.rdd.getNumPartitions(),
                'estimated_size_mb': estimated_size_mb,
                'data_analysis': analysis
            }
            
            if strategy == 'date_only':
                estimates['estimated_partitions'] = analysis['unique_dates']
            elif strategy == 'state_only':
                estimates['estimated_partitions'] = analysis['unique_states']
            elif strategy == 'hierarchical':
                estimates['estimated_partitions'] = min(
                    analysis['unique_dates'] * analysis['unique_states'],
                    self.partition_config['max_partitions_per_state'] * analysis['unique_states']
                )
            else:  # auto
                target_partitions = max(1, int(estimated_size_mb / self.partition_config['target_partition_size_mb']))
                estimates['estimated_partitions'] = target_partitions
            
            estimates['recommended_strategy'] = analysis['recommended_strategy']
            
            return estimates
            
        except Exception as e:
            self.logger.error("Failed to estimate partition count", exception=e)
            return {'error': str(e)}