# Architecture Système Global - Lakehouse Accidents US

## 🎯 Objectifs Architecturaux

**Analyses Métier Ciblées :**
- **Analyse Temporelle** : Patterns d'accidents par heure/jour/saison/conditions météo
- **Analyse Géographique** : Cartographie des zones à risque par type d'infrastructure

**Contraintes Techniques :**
- Cluster Hadoop on-premise (HDFS, Hive, Yarn)
- 32GB RAM total, 8GB max Spark
- Dataset 7.7M records, 47 colonnes, 3GB
- Ingestion mixte : streaming + batch + générateur de données
- PySpark uniquement, documentation française

## 🏗️ Architecture Système Global

```mermaid
graph TB
    subgraph "SOURCES DE DONNÉES"
        CSV[US_Accidents_March23.csv<br/>7.7M records, 3GB]
        GEN[Générateur Temps Réel<br/>Simulation nouvelles données]
        HIST[Données Historiques<br/>Retraitements batch]
    end

    subgraph "COUCHE BRONZE - HDFS"
        HDFS_RAW[HDFS Raw Data<br/>Parquet partitionné<br/>date/state]
        HDFS_STREAM[HDFS Streaming<br/>Micro-batches<br/>Delta format]
    end

    subgraph "APPLICATIONS SPARK"
        FEEDER[FEEDER<br/>Ingestion & Validation]
        PREPROCESSOR[PREPROCESSOR<br/>Nettoyage & Features]
        DATAMART[DATAMART<br/>Agrégations Business]
        MLTRAINING[MLTRAINING<br/>Pipeline ML]
    end

    subgraph "COUCHE SILVER - HIVE"
        HIVE_CLEAN[accidents_clean<br/>Données nettoyées]
        HIVE_WEATHER[weather_aggregated<br/>Agrégations météo]
        HIVE_INFRA[infrastructure_features<br/>Features infrastructure]
        HIVE_TEMPORAL[temporal_patterns<br/>Patterns temporels]
        HIVE_GEO[geographic_hotspots<br/>Zones à risque]
    end

    subgraph "COUCHE GOLD - MYSQL"
        MYSQL_KPI[KPIs Dashboard<br/>Métriques temps réel]
        MYSQL_HOTSPOTS[Hotspots Géographiques<br/>Zones critiques]
        MYSQL_TEMPORAL[Patterns Temporels<br/>Analyses saisonnières]
        MYSQL_PREDICTIONS[Prédictions ML<br/>Scores de risque]
    end

    subgraph "API & INTERFACES"
        API[FastAPI REST<br/>Swagger français]
        SWAGGER[Documentation API<br/>Endpoints métier]
    end

    subgraph "MONITORING & CONFIG"
        YARN[Yarn Resource Manager<br/>32GB RAM, 8GB Spark]
        LOG[Logging Structuré<br/>Monitoring pipeline]
        ENV[Configuration .env<br/>Variables externalisées]
    end

    CSV --> FEEDER
    GEN --> FEEDER
    HIST --> FEEDER
    
    FEEDER --> HDFS_RAW
    FEEDER --> HDFS_STREAM
    
    HDFS_RAW --> PREPROCESSOR
    HDFS_STREAM --> PREPROCESSOR
    
    PREPROCESSOR --> HIVE_CLEAN
    PREPROCESSOR --> HIVE_WEATHER
    PREPROCESSOR --> HIVE_INFRA
    PREPROCESSOR --> HIVE_TEMPORAL
    PREPROCESSOR --> HIVE_GEO
    
    HIVE_CLEAN --> DATAMART
    HIVE_WEATHER --> DATAMART
    HIVE_INFRA --> DATAMART
    HIVE_TEMPORAL --> DATAMART
    HIVE_GEO --> DATAMART
    
    DATAMART --> MYSQL_KPI
    DATAMART --> MYSQL_HOTSPOTS
    DATAMART --> MYSQL_TEMPORAL
    
    HIVE_CLEAN --> MLTRAINING
    MLTRAINING --> MYSQL_PREDICTIONS
    
    MYSQL_KPI --> API
    MYSQL_HOTSPOTS --> API
    MYSQL_TEMPORAL --> API
    MYSQL_PREDICTIONS --> API
    
    API --> SWAGGER
    
    YARN -.-> FEEDER
    YARN -.-> PREPROCESSOR
    YARN -.-> DATAMART
    YARN -.-> MLTRAINING
    
    LOG -.-> FEEDER
    LOG -.-> PREPROCESSOR
    LOG -.-> DATAMART
    LOG -.-> MLTRAINING
    
    ENV -.-> API
    ENV -.-> FEEDER
    ENV -.-> PREPROCESSOR
    ENV -.-> DATAMART
    ENV -.-> MLTRAINING
```

## 📊 Flux de Données Détaillé

### Couche Bronze (HDFS)
- **Format** : Parquet avec compression Snappy
- **Partitioning** : Par date (YYYY/MM/DD) et état (state)
- **Validation** : Schéma strict des 47 colonnes
- **Rétention** : 2 ans de données historiques

### Couche Silver (Hive)
- **Tables principales** :
  - `accidents_clean` : Données nettoyées et enrichies
  - `weather_aggregated` : Agrégations météorologiques
  - `infrastructure_features` : Features d'infrastructure
  - `temporal_patterns` : Patterns temporels
  - `geographic_hotspots` : Zones à risque géographiques

### Couche Gold (MySQL)
- **Tables optimisées** pour les requêtes API
- **Indexation** sur les colonnes de filtrage fréquentes
- **Agrégations pré-calculées** pour les KPIs
- **Mise à jour** quotidienne via les applications Spark

## 🔄 Applications Spark

### 1. FEEDER
- **Responsabilité** : Ingestion et validation des données
- **Fréquence** : Quotidienne + streaming temps réel
- **Optimisations** : Partitioning intelligent, validation schéma

### 2. PREPROCESSOR
- **Responsabilité** : Nettoyage et feature engineering
- **Fréquence** : Après chaque ingestion
- **Optimisations** : Cache des DataFrames, broadcast des tables de référence

### 3. DATAMART
- **Responsabilité** : Agrégations business et export MySQL
- **Fréquence** : Quotidienne
- **Optimisations** : Bucketing pour jointures, coalesce pour optimiser les fichiers

### 4. MLTRAINING
- **Responsabilité** : Entraînement modèles ML et prédictions
- **Fréquence** : Hebdomadaire pour réentraînement
- **Optimisations** : Cache du dataset d'entraînement, repartition équilibrée

## 🎯 Justifications Architecturales

### Choix du Lakehouse
- **Flexibilité** : Support des données structurées et semi-structurées
- **Performance** : Optimisations Spark pour les gros volumes
- **Évolutivité** : Architecture modulaire extensible
- **Coût** : Utilisation optimale des ressources Hadoop existantes

### Stratégie Multi-Couches
- **Bronze** : Données brutes pour audit et retraitement
- **Silver** : Données nettoyées pour analyses avancées
- **Gold** : Données agrégées pour performance API

### Choix Technologiques
- **PySpark** : Écosystème Python riche pour ML et analyse
- **Hive** : Intégration native avec Hadoop, métadonnées centralisées
- **MySQL** : Performance pour requêtes OLTP de l'API
- **FastAPI** : Performance et documentation automatique