# Application PREPROCESSOR

Application de transformation et nettoyage des données US-Accidents pour la couche Silver du lakehouse.

## 📋 Vue d'ensemble

L'application PREPROCESSOR transforme les données brutes de la couche Bronze vers la couche Silver avec un feature engineering avancé et la création de tables Hive optimisées.

### Fonctionnalités principales

- **Nettoyage des données** : Gestion des nulls, doublons, outliers pour les 47 colonnes
- **Normalisation** : Standardisation des formats et noms de colonnes
- **Feature Engineering** : Création de features temporelles, météorologiques, géographiques et d'infrastructure
- **Tables Hive** : Création de tables optimisées avec bucketing et partitioning
- **Agrégations** : Analyses avancées et métriques complexes

## 🏗️ Architecture

```
src/applications/preprocessor/
├── __init__.py                    # Module principal
├── preprocessor_app.py            # Application principale
├── data_cleaner.py               # Nettoyage des données
├── feature_engineer.py           # Feature engineering
├── hive_table_manager.py         # Gestion tables Hive
├── transformation_factory.py     # Factory transformations
├── aggregation_engine.py         # Moteur agrégations
├── test_preprocessor.py          # Tests complets
└── README.md                     # Documentation
```

## 🔧 Composants

### PreprocessorApp
Application principale orchestrant le pipeline de transformation.

**Fonctionnalités :**
- Lecture données Bronze (HDFS Parquet)
- Orchestration des transformations
- Gestion des erreurs et retry
- Métriques et logging détaillé

### DataCleaner
Composant de nettoyage et normalisation des données.

**Transformations :**
- Suppression des doublons basée sur l'ID
- Gestion des valeurs nulles par stratégies
- Normalisation des colonnes avec parenthèses
- Correction des données incohérentes
- Suppression des outliers statistiques

**Colonnes normalisées :**
```python
{
    'Distance(mi)': 'distance_miles',
    'Temperature(F)': 'temperature_fahrenheit',
    'Wind_Chill(F)': 'wind_chill_fahrenheit',
    'Humidity(%)': 'humidity_percent',
    'Pressure(in)': 'pressure_inches',
    'Visibility(mi)': 'visibility_miles',
    'Wind_Speed(mph)': 'wind_speed_mph',
    'Precipitation(in)': 'precipitation_inches'
}
```

### FeatureEngineer
Composant de feature engineering avancé.

**Features temporelles :**
- `accident_hour` (0-23)
- `accident_day_of_week` (1-7)
- `accident_month` (1-12)
- `accident_season` (Spring/Summer/Fall/Winter)
- `is_weekend` (boolean)
- `is_rush_hour` (boolean)

**Features météorologiques :**
- `weather_category` (Clear/Rain/Snow/Fog/Other)
- `weather_severity_score` (0-10)
- `visibility_category` (Good/Fair/Poor)
- `temperature_category` (Cold/Mild/Hot)

**Features géographiques :**
- `distance_to_city_center` (miles)
- `urban_rural_classification`
- `state_region` (Northeast/Southeast/Midwest/Southwest/West)
- `population_density_category`

**Features infrastructure :**
- `infrastructure_count` (somme des 13 variables)
- `safety_equipment_score` (pondération des équipements)
- `traffic_control_type` (Stop/Signal/None/Multiple)

### HiveTableManager
Gestionnaire des tables Hive optimisées.

**Tables créées :**

#### accidents_clean
- **Description** : Table principale avec toutes les données enrichies
- **Format** : ORC avec compression Snappy
- **Bucketing** : 50 buckets par State
- **Partitioning** : Par année et mois d'accident
- **Colonnes** : 47 originales + features créées

#### weather_aggregated
- **Description** : Agrégations météorologiques par zone/période
- **Format** : ORC avec compression Snappy
- **Bucketing** : 20 buckets par State
- **Partitioning** : Par année d'agrégation
- **Métriques** : Statistiques température, humidité, visibilité

#### infrastructure_features
- **Description** : Analyse équipements par zone géographique
- **Format** : ORC avec compression Snappy
- **Bucketing** : 20 buckets par State
- **Partitioning** : Par année d'analyse
- **Métriques** : Scores de sécurité, taux d'accidents par équipement

### TransformationFactory
Factory pour la création et gestion des transformations.

**Transformations disponibles :**
- `NullHandlingTransformation` : Gestion des valeurs nulles
- `OutlierRemovalTransformation` : Suppression des outliers
- `DataTypeConversionTransformation` : Conversion des types
- `StringNormalizationTransformation` : Normalisation des chaînes
- `RangeValidationTransformation` : Validation des plages

### AggregationEngine
Moteur d'agrégations avancées pour l'analyse.

**Types d'agrégations :**
- **Temporelles** : Par heure, jour, mois, saison
- **Géographiques** : Par état, région, zone urbaine/rurale
- **Météorologiques** : Par condition, température, visibilité
- **Infrastructure** : Par équipement, score de sécurité
- **Analyses avancées** : Percentiles, corrélations, rankings

## 🚀 Utilisation

### Exécution de base

```python
from src.applications.preprocessor import PreprocessorApp

# Création et exécution
app = PreprocessorApp()
result = app.run()

if result['status'] == 'success':
    print(f"✅ Preprocessing réussi: {result['metrics']['records_written']} enregistrements")
    print(f"📊 Features créées: {result['metrics']['features_created']}")
    print(f"🗃️ Tables créées: {result['metrics']['tables_created']}")
else:
    print(f"❌ Échec: {result['message']}")
```

### Utilisation des composants individuels

```python
from src.applications.preprocessor import DataCleaner, FeatureEngineer

# Nettoyage des données
cleaner = DataCleaner()
cleaned_df = cleaner.clean_data(raw_df)
cleaning_metrics = cleaner.get_cleaning_metrics()

# Feature engineering
engineer = FeatureEngineer()
enriched_df = engineer.create_features(cleaned_df)
feature_metrics = engineer.get_feature_metrics()
```

### Configuration personnalisée

```python
from src.common.config.config_manager import ConfigManager

# Configuration personnalisée
config = ConfigManager()
config._config.update({
    'hdfs': {
        'bronze_path': '/custom/bronze/path',
        'silver_path': '/custom/silver/path'
    },
    'hive': {
        'database': 'custom_accidents_db'
    }
})

app = PreprocessorApp(config)
```

## 📊 Métriques et Monitoring

### Métriques d'exécution
```python
{
    'records_read': 7700000,
    'records_processed': 7650000,
    'records_written': 7650000,
    'features_created': 18,
    'tables_created': 3,
    'cleaning_score': 88.5,
    'transformation_score': 92.3,
    'duration_seconds': 1245.6
}
```

### Métriques de nettoyage
```python
{
    'records_input': 7700000,
    'records_output': 7650000,
    'duplicates_removed': 25000,
    'nulls_handled': 150000,
    'outliers_removed': 75000,
    'columns_normalized': 8,
    'cleaning_score': 88.5
}
```

### Métriques de feature engineering
```python
{
    'features_created': 18,
    'temporal_features': 6,
    'weather_features': 4,
    'geographic_features': 4,
    'infrastructure_features': 3,
    'categorical_encodings': 5,
    'feature_engineering_score': 92.3
}
```

## 🧪 Tests

### Exécution des tests

```bash
# Tests complets
python src/applications/preprocessor/test_preprocessor.py

# Tests avec couverture
pytest src/applications/preprocessor/test_preprocessor.py --cov=src.applications.preprocessor --cov-report=html

# Tests spécifiques
pytest src/applications/preprocessor/test_preprocessor.py::TestDataCleaner -v
```

### Types de tests
- **Tests unitaires** : Chaque composant individuellement
- **Tests d'intégration** : Interaction entre composants
- **Tests de performance** : Métriques et temps d'exécution
- **Tests de validation** : Qualité des données transformées

## ⚙️ Configuration

### Variables d'environnement

```bash
# Chemins HDFS
HDFS_BRONZE_PATH=/lakehouse/accidents/bronze
HDFS_SILVER_PATH=/lakehouse/accidents/silver

# Configuration Hive
HIVE_DATABASE=accidents_warehouse
HIVE_METASTORE_URI=thrift://localhost:9083

# Configuration Spark
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=4
SPARK_SQL_ADAPTIVE_ENABLED=true
```

### Configuration Hive

```yaml
hive:
  database: accidents_warehouse
  warehouse_dir: /user/hive/warehouse
  tables:
    accidents_clean: accidents_clean
    weather_aggregated: weather_aggregated
    infrastructure_features: infrastructure_features
```

## 🔍 Optimisations

### Optimisations Spark
- **Adaptive Query Execution** : Optimisation automatique des requêtes
- **Bucketing** : Distribution optimale des données
- **Vectorisation ORC** : Lecture vectorisée pour de meilleures performances
- **Cache intelligent** : Mise en cache basée sur la taille des données
- **Coalescing** : Évitement des petits fichiers

### Optimisations Hive
- **Format ORC** : Compression et indexation optimales
- **Partitioning** : Pruning des partitions pour les requêtes
- **Bucketing** : Jointures optimisées
- **Statistiques** : Optimisation du plan d'exécution
- **Bloom filters** : Filtrage rapide des données

## 📈 Performance

### Benchmarks typiques
- **Données d'entrée** : 7.7M enregistrements, 47 colonnes
- **Temps de traitement** : ~20 minutes (cluster 4 nœuds)
- **Réduction de taille** : ~15% après nettoyage
- **Features créées** : 18 nouvelles colonnes
- **Tables Hive** : 3 tables optimisées

### Recommandations
- **Mémoire executor** : Minimum 4GB par executor
- **Partitions Spark** : 200-400 partitions pour 7.7M enregistrements
- **Cache** : Activer pour les DataFrames réutilisés
- **Compression** : Snappy pour équilibre performance/taille

## 🚨 Gestion d'erreurs

### Types d'erreurs gérées
- **Erreurs Spark** : Session, configuration, ressources
- **Erreurs de données** : Schéma, qualité, corruption
- **Erreurs Hive** : Connexion, permissions, espace disque
- **Erreurs de transformation** : Types, valeurs, logique métier

### Stratégies de récupération
- **Retry automatique** : 3 tentatives avec backoff exponentiel
- **Isolation des erreurs** : Continuation malgré les erreurs non-critiques
- **Rollback** : Nettoyage en cas d'échec complet
- **Logging détaillé** : Traçabilité complète des erreurs

## 📚 Références

### Documentation technique
- [Architecture Lakehouse](../../docs/architecture_systeme_global.md)
- [Optimisations Spark](../../docs/spark_optimization.md)
- [Patterns de conception](../../docs/design_patterns_classes.md)
- [Logging et monitoring](../../docs/logging_monitoring.md)

### Standards de données
- [Schéma US-Accidents](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
- [Codes d'état américains](https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations)
- [Conditions météorologiques](https://www.weather.gov/bgm/forecast_terms)

---

**Version** : 1.0.0  
**Auteur** : Lakehouse Team  
**Dernière mise à jour** : 2025-06-19