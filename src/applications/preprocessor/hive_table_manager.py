"""
Composant HiveTableManager pour la création et gestion des tables Hive optimisées
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, year, month, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.exceptions.custom_exceptions import DatabaseOperationError


class HiveTableManager:
    """
    Gestionnaire des tables Hive pour la couche Silver
    
    Fonctionnalités:
    - Création de la table accidents_clean avec bucketing et partitioning
    - Création de la table weather_aggregated avec agrégations météo
    - Création de la table infrastructure_features avec analyse équipements
    - Format ORC avec compression et vectorisation
    - Optimisations Spark (bucketing, partitioning, indexing)
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le gestionnaire de tables Hive"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("hive_table_manager", self.config_manager)
        
        # Configuration Hive
        self.hive_database = self.config_manager.get('hive.database', 'accidents_warehouse')
        self.warehouse_dir = self.config_manager.get('hive.warehouse_dir', '/user/hive/warehouse')
        
        # Noms des tables
        self.table_names = {
            'accidents_clean': self.config_manager.get_hive_table_name('accidents_clean'),
            'weather_aggregated': self.config_manager.get_hive_table_name('weather_aggregated'),
            'infrastructure_features': self.config_manager.get_hive_table_name('infrastructure_features')
        }
        
        # Configuration bucketing
        self.bucketing_config = {
            'accidents_clean': {
                'columns': ['State'],
                'num_buckets': 50
            },
            'weather_aggregated': {
                'columns': ['State'],
                'num_buckets': 20
            },
            'infrastructure_features': {
                'columns': ['State'],
                'num_buckets': 20
            }
        }
        
        # Configuration partitioning
        self.partition_config = {
            'accidents_clean': ['accident_year', 'accident_month'],
            'weather_aggregated': ['aggregation_year'],
            'infrastructure_features': ['analysis_year']
        }
    
    def create_accidents_clean_table(self, df: DataFrame) -> Dict[str, Any]:
        """
        Crée la table accidents_clean avec toutes les données enrichies
        
        Args:
            df: DataFrame avec les données nettoyées et enrichies
            
        Returns:
            Dict avec le résultat de la création
        """
        try:
            self.logger.info("Creating accidents_clean table", 
                           table_name=self.table_names['accidents_clean'])
            
            # Préparation des données avec colonnes de partitioning
            prepared_df = self._prepare_accidents_data(df)
            
            # Création du schéma optimisé
            schema = self._get_accidents_clean_schema()
            
            # Écriture avec optimisations
            result = self._write_bucketed_table(
                df=prepared_df,
                table_name='accidents_clean',
                schema=schema,
                partition_columns=self.partition_config['accidents_clean']
            )
            
            # Création des index et statistiques
            self._create_table_statistics('accidents_clean')
            
            self.logger.log_database_operation(
                operation="create_table",
                table=self.table_names['accidents_clean'],
                rows_affected=result.get('records_written', 0)
            )
            
            return result
            
        except Exception as e:
            self.logger.error("Failed to create accidents_clean table", exception=e)
            raise DatabaseOperationError(
                message=f"Failed to create accidents_clean table: {str(e)}",
                operation="create_table",
                table=self.table_names['accidents_clean']
            )
    
    def create_weather_aggregated_table(self, df: DataFrame) -> Dict[str, Any]:
        """
        Crée la table weather_aggregated avec agrégations météorologiques
        
        Args:
            df: DataFrame avec les données enrichies
            
        Returns:
            Dict avec le résultat de la création
        """
        try:
            self.logger.info("Creating weather_aggregated table", 
                           table_name=self.table_names['weather_aggregated'])
            
            # Création des agrégations météo
            weather_agg_df = self._create_weather_aggregations(df)
            
            # Création du schéma
            schema = self._get_weather_aggregated_schema()
            
            # Écriture avec optimisations
            result = self._write_bucketed_table(
                df=weather_agg_df,
                table_name='weather_aggregated',
                schema=schema,
                partition_columns=self.partition_config['weather_aggregated']
            )
            
            # Création des statistiques
            self._create_table_statistics('weather_aggregated')
            
            self.logger.log_database_operation(
                operation="create_table",
                table=self.table_names['weather_aggregated'],
                rows_affected=result.get('records_written', 0)
            )
            
            return result
            
        except Exception as e:
            self.logger.error("Failed to create weather_aggregated table", exception=e)
            raise DatabaseOperationError(
                message=f"Failed to create weather_aggregated table: {str(e)}",
                operation="create_table",
                table=self.table_names['weather_aggregated']
            )
    
    def create_infrastructure_features_table(self, df: DataFrame) -> Dict[str, Any]:
        """
        Crée la table infrastructure_features avec analyse des équipements
        
        Args:
            df: DataFrame avec les données enrichies
            
        Returns:
            Dict avec le résultat de la création
        """
        try:
            self.logger.info("Creating infrastructure_features table", 
                           table_name=self.table_names['infrastructure_features'])
            
            # Création des analyses d'infrastructure
            infra_df = self._create_infrastructure_analysis(df)
            
            # Création du schéma
            schema = self._get_infrastructure_features_schema()
            
            # Écriture avec optimisations
            result = self._write_bucketed_table(
                df=infra_df,
                table_name='infrastructure_features',
                schema=schema,
                partition_columns=self.partition_config['infrastructure_features']
            )
            
            # Création des statistiques
            self._create_table_statistics('infrastructure_features')
            
            self.logger.log_database_operation(
                operation="create_table",
                table=self.table_names['infrastructure_features'],
                rows_affected=result.get('records_written', 0)
            )
            
            return result
            
        except Exception as e:
            self.logger.error("Failed to create infrastructure_features table", exception=e)
            raise DatabaseOperationError(
                message=f"Failed to create infrastructure_features table: {str(e)}",
                operation="create_table",
                table=self.table_names['infrastructure_features']
            )
    
    def _prepare_accidents_data(self, df: DataFrame) -> DataFrame:
        """Prépare les données pour la table accidents_clean"""
        try:
            # Ajout des colonnes de partitioning
            prepared_df = df
            
            if 'Start_Time' in df.columns:
                prepared_df = prepared_df.withColumn('accident_year', year(col('Start_Time')))
                prepared_df = prepared_df.withColumn('accident_month', month(col('Start_Time')))
            
            # Sélection et réorganisation des colonnes
            selected_columns = self._get_accidents_clean_columns(prepared_df)
            prepared_df = prepared_df.select(*selected_columns)
            
            return prepared_df
            
        except Exception as e:
            self.logger.error("Failed to prepare accidents data", exception=e)
            return df
    
    def _create_weather_aggregations(self, df: DataFrame) -> DataFrame:
        """Crée les agrégations météorologiques"""
        try:
            # Vérification des colonnes nécessaires
            required_columns = ['State', 'weather_category', 'temperature_fahrenheit', 
                              'humidity_percent', 'visibility_miles', 'Start_Time']
            
            available_columns = [col for col in required_columns if col in df.columns]
            
            if len(available_columns) < 3:
                self.logger.warning("Insufficient columns for weather aggregation")
                return df.limit(0)  # DataFrame vide avec schéma
            
            # Agrégations par état et condition météo
            weather_agg = df.groupBy('State', 'weather_category') \
                .agg(
                    count('*').alias('accident_count'),
                    avg('temperature_fahrenheit').alias('avg_temperature'),
                    avg('humidity_percent').alias('avg_humidity'),
                    avg('visibility_miles').alias('avg_visibility'),
                    spark_min('temperature_fahrenheit').alias('min_temperature'),
                    spark_max('temperature_fahrenheit').alias('max_temperature'),
                    year(col('Start_Time')).alias('aggregation_year')
                )
            
            return weather_agg
            
        except Exception as e:
            self.logger.error("Failed to create weather aggregations", exception=e)
            return df.limit(0)
    
    def _create_infrastructure_analysis(self, df: DataFrame) -> DataFrame:
        """Crée l'analyse des équipements d'infrastructure"""
        try:
            # Colonnes d'infrastructure disponibles
            infrastructure_columns = [
                'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
                'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming',
                'Traffic_Signal', 'Turning_Loop'
            ]
            
            available_infra_cols = [col for col in infrastructure_columns if col in df.columns]
            
            if not available_infra_cols:
                self.logger.warning("No infrastructure columns found")
                return df.limit(0)
            
            # Agrégations par état et zone géographique
            base_columns = ['State']
            if 'urban_rural_classification' in df.columns:
                base_columns.append('urban_rural_classification')
            
            # Calcul des statistiques d'infrastructure
            agg_exprs = [count('*').alias('total_accidents')]
            
            for col_name in available_infra_cols:
                agg_exprs.extend([
                    spark_sum(col(col_name).cast('int')).alias(f'{col_name.lower()}_count'),
                    avg(col(col_name).cast('int')).alias(f'{col_name.lower()}_rate')
                ])
            
            if 'safety_equipment_score' in df.columns:
                agg_exprs.extend([
                    avg('safety_equipment_score').alias('avg_safety_score'),
                    spark_max('safety_equipment_score').alias('max_safety_score')
                ])
            
            if 'Start_Time' in df.columns:
                agg_exprs.append(year(col('Start_Time')).alias('analysis_year'))
            
            infra_analysis = df.groupBy(*base_columns).agg(*agg_exprs)
            
            return infra_analysis
            
        except Exception as e:
            self.logger.error("Failed to create infrastructure analysis", exception=e)
            return df.limit(0)
    
    def _write_bucketed_table(self, df: DataFrame, table_name: str, 
                             schema: StructType, partition_columns: List[str]) -> Dict[str, Any]:
        """Écrit une table avec bucketing et optimisations ORC"""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                raise DatabaseOperationError("No active Spark session")
            
            # Configuration bucketing
            bucket_config = self.bucketing_config[table_name]
            full_table_name = f"{self.hive_database}.{self.table_names[table_name]}"
            
            # Création de la base de données si nécessaire
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.hive_database}")
            
            # Suppression de la table existante
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            
            # Configuration du writer avec bucketing et partitioning
            writer = df.write \
                .mode('overwrite') \
                .format('orc') \
                .option('compression', 'snappy') \
                .option('orc.compress', 'SNAPPY') \
                .option('orc.create.index', 'true') \
                .option('orc.bloom.filter.columns', ','.join(bucket_config['columns'])) \
                .bucketBy(bucket_config['num_buckets'], *bucket_config['columns'])
            
            if partition_columns:
                available_partitions = [col for col in partition_columns if col in df.columns]
                if available_partitions:
                    writer = writer.partitionBy(*available_partitions)
            
            # Écriture de la table
            writer.saveAsTable(full_table_name)
            
            # Calcul des métriques
            records_written = df.count()
            
            # Optimisation post-création
            self._optimize_table(full_table_name)
            
            self.logger.info("Bucketed table created successfully", 
                           table_name=full_table_name,
                           records_written=records_written,
                           buckets=bucket_config['num_buckets'])
            
            return {
                'success': True,
                'table_name': full_table_name,
                'records_written': records_written,
                'buckets': bucket_config['num_buckets'],
                'partitions': partition_columns
            }
            
        except Exception as e:
            self.logger.error("Failed to write bucketed table", exception=e, table_name=table_name)
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name,
                'records_written': 0
            }
    
    def _optimize_table(self, table_name: str):
        """Optimise une table après création"""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                return
            
            # Analyse des statistiques de la table
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
            
            # Compactage des fichiers ORC
            spark.sql(f"ALTER TABLE {table_name} COMPACT 'major'")
            
            self.logger.info("Table optimized", table_name=table_name)
            
        except Exception as e:
            self.logger.warning("Table optimization failed", exception=e, table_name=table_name)
    
    def _create_table_statistics(self, table_key: str):
        """Crée les statistiques pour une table"""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                return
            
            full_table_name = f"{self.hive_database}.{self.table_names[table_key]}"
            
            # Statistiques de base
            spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS")
            
            # Statistiques par colonne pour les colonnes importantes
            important_columns = {
                'accidents_clean': ['State', 'Severity', 'weather_category'],
                'weather_aggregated': ['State', 'weather_category'],
                'infrastructure_features': ['State']
            }
            
            if table_key in important_columns:
                for column in important_columns[table_key]:
                    try:
                        spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR COLUMNS {column}")
                    except Exception:
                        pass  # Colonne peut ne pas exister
            
            self.logger.info("Table statistics created", table_name=full_table_name)
            
        except Exception as e:
            self.logger.warning("Statistics creation failed", exception=e, table_key=table_key)
    
    def _get_accidents_clean_schema(self) -> StructType:
        """Retourne le schéma pour la table accidents_clean"""
        return StructType([
            # Colonnes originales principales
            StructField("ID", StringType(), False),
            StructField("Severity", IntegerType(), True),
            StructField("Start_Time", TimestampType(), True),
            StructField("End_Time", TimestampType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("State", StringType(), True),
            StructField("City", StringType(), True),
            StructField("County", StringType(), True),
            
            # Colonnes normalisées
            StructField("distance_miles", DoubleType(), True),
            StructField("temperature_fahrenheit", DoubleType(), True),
            StructField("humidity_percent", DoubleType(), True),
            StructField("visibility_miles", DoubleType(), True),
            StructField("wind_speed_mph", DoubleType(), True),
            
            # Features temporelles
            StructField("accident_hour", IntegerType(), True),
            StructField("accident_day_of_week", IntegerType(), True),
            StructField("accident_month", IntegerType(), True),
            StructField("accident_season", StringType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("is_rush_hour", BooleanType(), True),
            
            # Features météorologiques
            StructField("weather_category", StringType(), True),
            StructField("weather_severity_score", IntegerType(), True),
            StructField("visibility_category", StringType(), True),
            StructField("temperature_category", StringType(), True),
            
            # Features géographiques
            StructField("distance_to_city_center", DoubleType(), True),
            StructField("urban_rural_classification", StringType(), True),
            StructField("state_region", StringType(), True),
            
            # Features infrastructure
            StructField("infrastructure_count", IntegerType(), True),
            StructField("safety_equipment_score", IntegerType(), True),
            StructField("traffic_control_type", StringType(), True),
            
            # Colonnes de partitioning
            StructField("accident_year", IntegerType(), True),
            StructField("accident_month", IntegerType(), True)
        ])
    
    def _get_weather_aggregated_schema(self) -> StructType:
        """Retourne le schéma pour la table weather_aggregated"""
        return StructType([
            StructField("State", StringType(), True),
            StructField("weather_category", StringType(), True),
            StructField("accident_count", IntegerType(), True),
            StructField("avg_temperature", DoubleType(), True),
            StructField("avg_humidity", DoubleType(), True),
            StructField("avg_visibility", DoubleType(), True),
            StructField("min_temperature", DoubleType(), True),
            StructField("max_temperature", DoubleType(), True),
            StructField("aggregation_year", IntegerType(), True)
        ])
    
    def _get_infrastructure_features_schema(self) -> StructType:
        """Retourne le schéma pour la table infrastructure_features"""
        return StructType([
            StructField("State", StringType(), True),
            StructField("urban_rural_classification", StringType(), True),
            StructField("total_accidents", IntegerType(), True),
            StructField("avg_safety_score", DoubleType(), True),
            StructField("max_safety_score", IntegerType(), True),
            StructField("analysis_year", IntegerType(), True)
        ])
    
    def _get_accidents_clean_columns(self, df: DataFrame) -> List[str]:
        """Retourne la liste des colonnes pour accidents_clean"""
        # Colonnes de base toujours incluses
        base_columns = ['ID', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng', 
                       'State', 'City', 'County']
        
        # Colonnes optionnelles selon disponibilité
        optional_columns = [
            'distance_miles', 'temperature_fahrenheit', 'humidity_percent', 
            'visibility_miles', 'wind_speed_mph', 'accident_hour', 'accident_day_of_week',
            'accident_month', 'accident_season', 'is_weekend', 'is_rush_hour',
            'weather_category', 'weather_severity_score', 'visibility_category',
            'temperature_category', 'distance_to_city_center', 'urban_rural_classification',
            'state_region', 'infrastructure_count', 'safety_equipment_score',
            'traffic_control_type', 'accident_year', 'accident_month'
        ]
        
        # Sélection des colonnes disponibles
        available_columns = []
        for col in base_columns + optional_columns:
            if col in df.columns:
                available_columns.append(col)
        
        return available_columns
    
    def get_table_info(self, table_key: str) -> Dict[str, Any]:
        """Retourne les informations sur une table"""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                return {'error': 'No active Spark session'}
            
            full_table_name = f"{self.hive_database}.{self.table_names[table_key]}"
            
            # Informations de base
            table_info = {
                'table_name': full_table_name,
                'database': self.hive_database,
                'bucketing': self.bucketing_config[table_key],
                'partitioning': self.partition_config[table_key]
            }
            
            # Statistiques si la table existe
            try:
                stats_df = spark.sql(f"DESCRIBE EXTENDED {full_table_name}")
                table_info['exists'] = True
                table_info['statistics'] = [row.asDict() for row in stats_df.collect()]
            except Exception:
                table_info['exists'] = False
            
            return table_info
            
        except Exception as e:
            self.logger.error("Failed to get table info", exception=e, table_key=table_key)
            return {'error': str(e)}