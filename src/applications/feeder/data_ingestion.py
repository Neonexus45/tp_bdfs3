"""
Composant DataIngestion pour la lecture optimisée des données CSV US-Accidents
"""

import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, current_timestamp

from ...common.config.config_manager import ConfigManager
from ...common.utils.logger import Logger
from ...common.utils.validation_utils import ValidationUtils
from ...common.exceptions.custom_exceptions import FileProcessingError, DataValidationError


class DataIngestion:
    """
    Composant de lecture et ingestion des données CSV avec optimisations Spark
    
    Fonctionnalités:
    - Lecture CSV avec schéma défini (47 colonnes exactes)
    - Gestion des types de données (string, double, boolean, timestamp)
    - Optimisations lecture (multiline, encoding, delimiter)
    - Cache intelligent des DataFrames
    - Validation de l'existence et taille des fichiers
    """
    
    def __init__(self, config_manager: ConfigManager = None):
        """Initialise le composant DataIngestion"""
        self.config_manager = config_manager or ConfigManager()
        self.logger = Logger("data_ingestion", self.config_manager)
        self.validation_utils = ValidationUtils(self.config_manager)
        
        # Configuration de lecture CSV
        self.csv_options = {
            'header': 'true',
            'inferSchema': 'false',  # On utilise un schéma strict
            'multiline': 'true',
            'escape': '"',
            'quote': '"',
            'delimiter': ',',
            'encoding': 'UTF-8',
            'timestampFormat': 'yyyy-MM-dd HH:mm:ss',
            'dateFormat': 'yyyy-MM-dd',
            'nullValue': '',
            'emptyValue': '',
            'nanValue': 'NaN'
        }
        
        # Schéma US-Accidents (47 colonnes exactes)
        self.us_accidents_schema = self.validation_utils.get_us_accidents_schema()
        
        # Métriques de lecture
        self.ingestion_metrics = {
            'file_size_bytes': 0,
            'file_size_mb': 0.0,
            'records_read': 0,
            'columns_read': 0,
            'read_duration_seconds': 0.0,
            'cache_applied': False,
            'schema_enforced': True
        }
    
    def read_csv_data(self, file_path: str, schema: StructType = None) -> DataFrame:
        """
        Lit les données CSV avec schéma strict et optimisations
        
        Args:
            file_path: Chemin vers le fichier CSV
            schema: Schéma optionnel (utilise US-Accidents par défaut)
            
        Returns:
            DataFrame Spark avec les données lues et optimisées
            
        Raises:
            FileProcessingError: Si le fichier n'existe pas ou est corrompu
            DataValidationError: Si le schéma ne correspond pas
        """
        import time
        start_time = time.time()
        
        try:
            # Validation du fichier source
            self._validate_source_file(file_path)
            
            # Utilisation du schéma par défaut si non fourni
            if schema is None:
                schema = self.us_accidents_schema
            
            self.logger.info("Starting CSV data ingestion", 
                           file_path=file_path,
                           expected_columns=len(schema.fields))
            
            # Lecture avec schéma strict
            spark = SparkSession.getActiveSession()
            if not spark:
                raise FileProcessingError(
                    message="No active Spark session",
                    file_path=file_path,
                    operation="read_csv"
                )
            
            # Configuration du reader avec options optimisées
            reader = spark.read.format("csv")
            for option_key, option_value in self.csv_options.items():
                reader = reader.option(option_key, option_value)
            
            # Application du schéma strict
            reader = reader.schema(schema)
            
            # Lecture du fichier
            df = reader.load(file_path)
            
            # Ajout de métadonnées d'ingestion
            df_with_metadata = self._add_ingestion_metadata(df)
            
            # Cache intelligent si le fichier est de taille raisonnable
            cached_df = self._apply_intelligent_cache(df_with_metadata)
            
            # Calcul des métriques
            self._calculate_ingestion_metrics(cached_df, file_path, start_time)
            
            # Validation post-lecture
            self._validate_read_data(cached_df)
            
            self.logger.log_performance(
                operation="csv_data_ingestion",
                duration_seconds=self.ingestion_metrics['read_duration_seconds'],
                file_size_mb=self.ingestion_metrics['file_size_mb'],
                records_read=self.ingestion_metrics['records_read'],
                columns_read=self.ingestion_metrics['columns_read']
            )
            
            return cached_df
            
        except Exception as e:
            self.logger.error("CSV data ingestion failed", 
                            exception=e, 
                            file_path=file_path)
            
            if isinstance(e, (FileProcessingError, DataValidationError)):
                raise
            else:
                raise FileProcessingError(
                    message=f"Failed to read CSV data: {str(e)}",
                    file_path=file_path,
                    file_format="csv",
                    operation="read"
                )
    
    def _validate_source_file(self, file_path: str):
        """Valide l'existence et les propriétés du fichier source"""
        try:
            if not os.path.exists(file_path):
                raise FileProcessingError(
                    message=f"Source file does not exist: {file_path}",
                    file_path=file_path,
                    operation="validation"
                )
            
            # Vérification de la taille du fichier
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            self.ingestion_metrics['file_size_bytes'] = file_size
            self.ingestion_metrics['file_size_mb'] = round(file_size_mb, 2)
            
            # Vérification de la taille minimale (doit contenir des données)
            if file_size < 1024:  # Moins de 1KB
                raise FileProcessingError(
                    message=f"Source file too small: {file_size} bytes",
                    file_path=file_path,
                    file_size=file_size,
                    operation="validation"
                )
            
            # Vérification de la taille maximale (protection contre les fichiers énormes)
            max_size_gb = 10  # 10GB max
            if file_size_mb > max_size_gb * 1024:
                raise FileProcessingError(
                    message=f"Source file too large: {file_size_mb:.2f}MB (max: {max_size_gb}GB)",
                    file_path=file_path,
                    file_size=file_size,
                    operation="validation"
                )
            
            self.logger.info("Source file validated", 
                           file_path=file_path,
                           file_size_mb=file_size_mb)
            
        except OSError as e:
            raise FileProcessingError(
                message=f"File system error: {str(e)}",
                file_path=file_path,
                operation="validation"
            )
    
    def _add_ingestion_metadata(self, df: DataFrame) -> DataFrame:
        """Ajoute des métadonnées d'ingestion au DataFrame"""
        try:
            # Ajout de la date d'ingestion
            df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp())
            
            # Ajout de la date d'ingestion (pour partitioning)
            df_with_metadata = df_with_metadata.withColumn(
                "ingestion_date", 
                col("ingestion_timestamp").cast("date")
            )
            
            # Ajout d'un ID de batch d'ingestion
            import uuid
            batch_id = str(uuid.uuid4())
            df_with_metadata = df_with_metadata.withColumn("batch_id", lit(batch_id))
            
            self.logger.info("Ingestion metadata added", 
                           batch_id=batch_id,
                           metadata_columns=["ingestion_timestamp", "ingestion_date", "batch_id"])
            
            return df_with_metadata
            
        except Exception as e:
            self.logger.warning("Failed to add ingestion metadata", exception=e)
            return df
    
    def _apply_intelligent_cache(self, df: DataFrame) -> DataFrame:
        """Applique un cache intelligent basé sur la taille des données"""
        try:
            file_size_mb = self.ingestion_metrics['file_size_mb']
            
            # Cache seulement si le fichier est de taille raisonnable
            if file_size_mb < 1000:  # Moins de 1GB
                from pyspark.storagelevel import StorageLevel
                
                if file_size_mb < 100:  # Moins de 100MB -> MEMORY_ONLY
                    cached_df = df.persist(StorageLevel.MEMORY_ONLY)
                    cache_level = "MEMORY_ONLY"
                else:  # 100MB - 1GB -> MEMORY_AND_DISK
                    cached_df = df.persist(StorageLevel.MEMORY_AND_DISK)
                    cache_level = "MEMORY_AND_DISK"
                
                self.ingestion_metrics['cache_applied'] = True
                
                self.logger.info("Intelligent cache applied", 
                               cache_level=cache_level,
                               file_size_mb=file_size_mb)
                
                return cached_df
            else:
                self.logger.info("Cache skipped for large file", 
                               file_size_mb=file_size_mb)
                return df
                
        except Exception as e:
            self.logger.warning("Failed to apply cache", exception=e)
            return df
    
    def _calculate_ingestion_metrics(self, df: DataFrame, file_path: str, start_time: float):
        """Calcule les métriques d'ingestion"""
        try:
            import time
            end_time = time.time()
            
            # Métriques temporelles
            self.ingestion_metrics['read_duration_seconds'] = round(end_time - start_time, 2)
            
            # Métriques de données
            self.ingestion_metrics['records_read'] = df.count()
            self.ingestion_metrics['columns_read'] = len(df.columns)
            
            # Validation du nombre de colonnes attendu
            expected_columns = self.config_manager.get('data.expected_columns', 47)
            if self.ingestion_metrics['columns_read'] != expected_columns + 3:  # +3 pour les métadonnées
                self.logger.warning("Unexpected column count", 
                                  expected=expected_columns + 3,
                                  actual=self.ingestion_metrics['columns_read'])
            
        except Exception as e:
            self.logger.error("Failed to calculate ingestion metrics", exception=e)
    
    def _validate_read_data(self, df: DataFrame):
        """Valide les données lues"""
        try:
            # Vérification que le DataFrame n'est pas vide
            if self.ingestion_metrics['records_read'] == 0:
                raise DataValidationError(
                    message="No records found in source file",
                    validation_type="record_count",
                    dataset="us_accidents"
                )
            
            # Vérification du nombre minimum d'enregistrements
            min_records = 1000  # Au moins 1000 enregistrements
            if self.ingestion_metrics['records_read'] < min_records:
                self.logger.warning("Low record count detected", 
                                  records_read=self.ingestion_metrics['records_read'],
                                  min_expected=min_records)
            
            # Vérification des colonnes obligatoires
            required_columns = ['ID', 'Severity', 'Start_Time', 'Start_Lat', 'Start_Lng', 'State']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise DataValidationError(
                    message=f"Missing required columns: {missing_columns}",
                    validation_type="required_columns",
                    failed_rules=missing_columns,
                    dataset="us_accidents"
                )
            
            self.logger.info("Data validation passed", 
                           records_read=self.ingestion_metrics['records_read'],
                           columns_read=self.ingestion_metrics['columns_read'])
            
        except Exception as e:
            if isinstance(e, DataValidationError):
                raise
            else:
                self.logger.error("Data validation failed", exception=e)
                raise DataValidationError(
                    message=f"Data validation error: {str(e)}",
                    validation_type="post_read_validation",
                    dataset="us_accidents"
                )
    
    def read_with_custom_schema(self, file_path: str, custom_schema: StructType) -> DataFrame:
        """
        Lit les données avec un schéma personnalisé
        
        Args:
            file_path: Chemin vers le fichier CSV
            custom_schema: Schéma personnalisé à appliquer
            
        Returns:
            DataFrame avec le schéma personnalisé
        """
        self.logger.info("Reading with custom schema", 
                       file_path=file_path,
                       custom_columns=len(custom_schema.fields))
        
        return self.read_csv_data(file_path, custom_schema)
    
    def get_ingestion_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques d'ingestion"""
        return self.ingestion_metrics.copy()
    
    def estimate_processing_time(self, file_path: str) -> Dict[str, Any]:
        """
        Estime le temps de traitement basé sur la taille du fichier
        
        Args:
            file_path: Chemin vers le fichier à analyser
            
        Returns:
            Dict avec les estimations de temps
        """
        try:
            if not os.path.exists(file_path):
                return {'error': 'File not found'}
            
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            
            # Estimations basées sur des benchmarks (à ajuster selon l'environnement)
            read_time_per_mb = 2.0  # secondes par MB
            processing_time_per_mb = 1.5  # secondes par MB pour le traitement
            
            estimated_read_time = file_size_mb * read_time_per_mb
            estimated_processing_time = file_size_mb * processing_time_per_mb
            estimated_total_time = estimated_read_time + estimated_processing_time
            
            return {
                'file_size_mb': round(file_size_mb, 2),
                'estimated_read_time_seconds': round(estimated_read_time, 2),
                'estimated_processing_time_seconds': round(estimated_processing_time, 2),
                'estimated_total_time_seconds': round(estimated_total_time, 2),
                'estimated_total_time_minutes': round(estimated_total_time / 60, 2)
            }
            
        except Exception as e:
            self.logger.error("Failed to estimate processing time", exception=e)
            return {'error': str(e)}