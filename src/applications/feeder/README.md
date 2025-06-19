# FEEDER Application - Ingestion des Données US-Accidents

## Vue d'ensemble

L'application **FEEDER** est responsable de l'ingestion quotidienne des données US-Accidents dans l'architecture lakehouse. Elle implémente un pipeline complet d'ingestion avec validation, contrôle qualité et optimisations Spark.

## Fonctionnalités Principales

### 🔍 Lecture CSV Optimisée
- **Schéma strict** des 47 colonnes exactes du dataset US-Accidents
- **Gestion des types** (string, double, boolean, timestamp)
- **Optimisations lecture** (multiline, encoding, delimiter)
- **Cache intelligent** des DataFrames

### ✅ Validation de Schéma
- **Schéma US-Accidents pré-défini** avec types exacts
- **Validation structure** (nombre colonnes, noms, types)
- **Détection colonnes** manquantes/supplémentaires
- **Rapports de validation** détaillés

### 🔍 Contrôle Qualité
- **Validation coordonnées GPS** (latitude/longitude valides)
- **Vérification codes d'état** américains (State column)
- **Détection outliers** (Distance, Temperature, etc.)
- **Calcul métriques qualité** (% nulls, doublons, etc.)

### 📊 Partitioning Intelligent
- **Partitioning par date** d'ingestion (YYYY/MM/DD)
- **Sous-partitioning par état** (State column)
- **Optimisation nombre de partitions** (éviter small files)
- **Coalescing intelligent** avant écriture

### ⚡ Optimisations Spark
- **Partitioning**: 200 partitions optimales pour 3GB
- **Compression**: Snappy pour balance compression/vitesse
- **Cache**: DataFrames réutilisés en mémoire
- **Coalesce**: Éviter les petits fichiers (<128MB)

## Architecture des Composants

```
src/applications/feeder/
├── __init__.py                 # Exports et métadonnées
├── feeder_app.py              # Application principale
├── data_ingestion.py          # Logique ingestion
├── schema_validator.py        # Validation schéma
├── quality_checker.py         # Contrôle qualité
├── partitioning_strategy.py   # Stratégie partitioning
├── test_feeder.py            # Tests unitaires
└── README.md                 # Documentation
```

## Classes Principales

### 1. FeederApp
**Application principale** avec orchestration du pipeline d'ingestion.

```python
from src.applications.feeder import FeederApp

feeder = FeederApp()
result = feeder.run()
```

**Fonctionnalités:**
- Hérite de la classe de base SparkApplication
- Orchestration du pipeline d'ingestion
- Gestion des erreurs et retry automatique (3 tentatives)
- Métriques de performance et logging structuré

### 2. DataIngestion
**Lecture CSV** avec schéma défini et optimisations.

```python
from src.applications.feeder import DataIngestion

ingestion = DataIngestion()
df = ingestion.read_csv_data("path/to/file.csv")
```

**Fonctionnalités:**
- Lecture CSV avec schéma strict (47 colonnes)
- Gestion des types de données
- Cache intelligent basé sur la taille
- Validation post-lecture

### 3. SchemaValidator
**Validation stricte** du schéma US-Accidents.

```python
from src.applications.feeder import SchemaValidator

validator = SchemaValidator()
result = validator.validate_dataframe_schema(df)
```

**Fonctionnalités:**
- Schéma pré-défini avec types exacts
- Détection colonnes manquantes/supplémentaires
- Rapports de validation détaillés
- Mode strict configurable

### 4. QualityChecker
**Contrôle qualité** et nettoyage des données.

```python
from src.applications.feeder import QualityChecker

checker = QualityChecker()
quality_report = checker.check_data_quality(df)
cleaned_df = checker.clean_corrupted_data(df)
```

**Fonctionnalités:**
- Validation coordonnées GPS US
- Vérification codes d'état américains
- Détection outliers statistiques
- Score de qualité automatique

### 5. PartitioningStrategy
**Stratégie de partitioning** intelligent.

```python
from src.applications.feeder import PartitioningStrategy

strategy = PartitioningStrategy()
partitioned_df = strategy.apply_partitioning(df, 'auto')
```

**Fonctionnalités:**
- Partitioning par date et état
- Optimisation automatique du nombre de partitions
- Coalescing intelligent
- Stratégies configurables

## Configuration

### Variables d'Environnement

```bash
# Source des données
DATA_SOURCE_PATH=data/raw/US_Accidents_March23.csv
DATA_EXPECTED_COLUMNS=47
DATA_EXPECTED_ROWS=7700000

# Partitioning
PARTITION_COLUMNS=["date", "state"]
COMPRESSION_CODEC=snappy
PARQUET_COMPRESSION=snappy

# Chemins HDFS
HDFS_BRONZE_PATH=/lakehouse/accidents/bronze
HDFS_NAMENODE=hdfs://localhost:9000

# Spark
SPARK_SQL_SHUFFLE_PARTITIONS=200
SPARK_SQL_ADAPTIVE_ENABLED=true
```

### Configuration Spark Optimisée

L'application utilise une configuration Spark spécialement optimisée pour l'ingestion:

```python
spark_config = {
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.sql.files.maxPartitionBytes': '134217728',  # 128MB
    'spark.sql.adaptive.advisoryPartitionSizeInBytes': '67108864',  # 64MB
    'spark.sql.files.openCostInBytes': '4194304',  # 4MB
}
```

## Métriques et Logging

### Métriques Collectées
- **Nombre d'enregistrements** lus/écrits
- **Temps d'exécution** par étape
- **Taille fichiers** source/destination
- **Métriques qualité** (% erreurs, nulls, doublons)
- **Utilisation ressources** (CPU, mémoire, I/O)
- **Nombre de partitions** créées

### Format de Logging
Tous les logs sont structurés en JSON:

```json
{
  "timestamp": "2023-12-19T21:15:32Z",
  "level": "INFO",
  "application": "feeder",
  "operation": "csv_data_ingestion",
  "duration_seconds": 45.2,
  "records_read": 7700000,
  "file_size_mb": 3072.5,
  "quality_score": 87.3
}
```

## Gestion d'Erreurs

### Retry Automatique
- **3 tentatives maximum** avec backoff exponentiel
- **Isolation des enregistrements** corrompus
- **Logging détaillé** des erreurs avec contexte
- **Fallback vers mode dégradé** si nécessaire

### Types d'Erreurs Gérées
- `FileProcessingError`: Problèmes de lecture fichier
- `SchemaValidationError`: Schéma non conforme
- `DataQualityError`: Qualité insuffisante
- `SparkJobError`: Erreurs d'exécution Spark

## Tests

### Exécution des Tests

```bash
# Tests complets
python src/applications/feeder/test_feeder.py

# Tests individuels
python -c "from src.applications.feeder.test_feeder import FeederTester; t = FeederTester(); print(t.test_data_ingestion())"
```

### Types de Tests
- **Test DataIngestion**: Lecture CSV et métriques
- **Test SchemaValidator**: Validation schéma
- **Test QualityChecker**: Contrôle qualité
- **Test PartitioningStrategy**: Stratégies de partitioning
- **Test FeederApp**: Application complète

## Usage

### Exécution Simple

```python
from src.applications.feeder import FeederApp

# Création et exécution
feeder = FeederApp()
result = feeder.run()

if result['status'] == 'success':
    print(f"✅ Ingestion réussie: {result['metrics']['records_written']} enregistrements")
else:
    print(f"❌ Échec: {result['message']}")
```

### Exécution en Ligne de Commande

```bash
# Exécution directe
python src/applications/feeder/feeder_app.py

# Avec configuration personnalisée
DATA_SOURCE_PATH=/custom/path/data.csv python src/applications/feeder/feeder_app.py
```

### Utilisation des Composants Individuels

```python
from src.applications.feeder import DataIngestion, SchemaValidator, QualityChecker

# Pipeline personnalisé
ingestion = DataIngestion()
validator = SchemaValidator()
checker = QualityChecker()

# Lecture
df = ingestion.read_csv_data("data.csv")

# Validation
schema_result = validator.validate_dataframe_schema(df)
if not schema_result['is_valid']:
    raise Exception("Schéma invalide")

# Contrôle qualité
quality_result = checker.check_data_quality(df)
if quality_result['overall_quality_score'] < 70:
    df = checker.clean_corrupted_data(df)
```

## Performance

### Benchmarks Typiques
- **Fichier 3GB (7.7M enregistrements)**: ~5-8 minutes
- **Débit lecture**: ~400-600 MB/min
- **Partitions créées**: ~200-400 selon les données
- **Compression Snappy**: ~60-70% réduction taille

### Optimisations Recommandées
- **Mémoire Spark**: Minimum 4GB driver, 8GB executors
- **Partitions**: 200 partitions pour 3GB de données
- **Cache**: Activé pour DataFrames < 1GB
- **Coalescing**: Fichiers finaux ~128MB chacun

## Monitoring

### Métriques Clés à Surveiller
- **Temps d'exécution total** (SLA: < 10 minutes)
- **Score de qualité** (Seuil: > 80%)
- **Taux d'erreur** (Seuil: < 5%)
- **Utilisation mémoire** (Seuil: < 80%)

### Alertes Configurées
- Échec d'ingestion après 3 tentatives
- Score de qualité < 70%
- Temps d'exécution > 15 minutes
- Utilisation mémoire > 90%

## Dépannage

### Problèmes Courants

**1. Erreur de schéma**
```
SchemaValidationError: Missing required columns: ['ID', 'State']
```
→ Vérifier le format du fichier source et les 47 colonnes

**2. Qualité insuffisante**
```
DataQualityError: Quality score 45.2 below threshold 70.0
```
→ Examiner les métriques de qualité et nettoyer les données

**3. Erreur de partitioning**
```
SparkJobError: Partitioning failed: Invalid state codes
```
→ Vérifier les codes d'état américains dans les données

### Logs de Debug

```bash
# Activation du debug
LOG_FEEDER_LEVEL=DEBUG python src/applications/feeder/feeder_app.py

# Logs détaillés
tail -f /var/log/lakehouse-accidents/feeder.log
```

## Évolutions Futures

### Améliorations Prévues
- **Streaming**: Support ingestion temps réel
- **Delta Lake**: Migration vers format Delta
- **Auto-scaling**: Ajustement automatique des ressources
- **ML Quality**: Détection anomalies par ML

### Intégrations Planifiées
- **Apache Airflow**: Orchestration workflow
- **Prometheus**: Métriques avancées
- **Grafana**: Dashboards monitoring
- **Apache Atlas**: Gouvernance données