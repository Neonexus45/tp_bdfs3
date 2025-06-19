# DATAMART - Couche Gold Analytics

Application DATAMART pour la création des tables analytiques optimisées et l'export vers MySQL selon l'architecture Medallion Gold.

## 🎯 Objectif

L'application DATAMART constitue la couche **Gold** de l'architecture Medallion, transformant les données nettoyées de la couche Silver en tables analytiques optimisées pour les APIs et les tableaux de bord.

## 🏗️ Architecture Medallion - Couche Gold

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   BRONZE        │    │    SILVER       │    │     GOLD        │
│   (FEEDER)      │───▶│ (PREPROCESSOR)  │───▶│   (DATAMART)    │
│                 │    │                 │    │                 │
│ • Données brutes│    │ • Nettoyage     │    │ • KPIs business │
│ • HDFS Parquet  │    │ • Hive ORC      │    │ • MySQL tables  │
│ • Partitioning  │    │ • Features eng. │    │ • API optimisé  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 Fonctionnalités Principales

### 1. **Agrégations Business Multi-dimensionnelles**
- **Temporelles**: Patterns horaires, journaliers, mensuels, annuels
- **Géographiques**: Analyses par état, ville, coordonnées GPS
- **Météorologiques**: Corrélations conditions météo vs accidents
- **Infrastructure**: Efficacité des équipements de sécurité

### 2. **Calcul des KPIs Business**
- **KPIs Sécurité**: Taux d'accidents par 100k habitants, index dangerosité
- **KPIs Temporels**: Évolution mensuelle/annuelle, pics horaires, saisonnalité
- **KPIs Infrastructure**: Corrélation équipements vs accidents, efficacité
- **Hotspots**: Top 50 zones les plus dangereuses avec géolocalisation

### 3. **Export MySQL Optimisé**
- Tables dénormalisées pour performance API FastAPI
- Indexation composite intelligente pour requêtes fréquentes
- Bulk insert optimisé avec gestion des transactions
- Synchronisation incrémentale des données

### 4. **Optimisation Performance**
- **Spark**: Broadcast joins, cache, coalesce, partitioning
- **MySQL**: Index composites, partitioning par date, query optimization
- **API**: Tables pré-agrégées pour réponses sub-secondes

## 🗂️ Structure des Composants

```
src/applications/datamart/
├── __init__.py                 # Package initialization
├── datamart_app.py            # 🎯 Application principale
├── business_aggregator.py     # 📊 Agrégations multi-dimensionnelles
├── kpi_calculator.py          # 📈 Calcul des KPIs business
├── mysql_exporter.py          # 🗄️ Export optimisé MySQL
├── table_optimizer.py         # ⚡ Optimisation tables/requêtes
├── analytics_engine.py        # 🔍 Moteur d'analyse avancée
├── test_datamart.py          # 🧪 Tests complets
└── README.md                  # 📚 Documentation
```

## 🗄️ Tables MySQL Gold Créées

### 1. **accidents_summary** (Table principale dénormalisée)
```sql
CREATE TABLE accidents_summary (
    id VARCHAR(50) PRIMARY KEY,
    state VARCHAR(2) NOT NULL,
    city VARCHAR(100),
    severity INT,
    accident_date DATE,
    accident_hour INT,
    weather_category VARCHAR(20),
    temperature_category VARCHAR(10),
    infrastructure_count INT,
    safety_score DOUBLE,
    distance_miles DOUBLE,
    INDEX idx_state_date (state, accident_date),
    INDEX idx_severity_weather (severity, weather_category),
    INDEX idx_location (state, city),
    INDEX idx_temporal (accident_date, accident_hour)
) PARTITION BY RANGE (YEAR(accident_date));
```

### 2. **kpis_security** (KPIs sécurité pour /hotspots)
```sql
CREATE TABLE kpis_security (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(2) NOT NULL,
    city VARCHAR(100),
    accident_rate_per_100k DOUBLE,
    danger_index DOUBLE,
    severity_distribution JSON,
    hotspot_rank INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_state (state),
    INDEX idx_danger_index (danger_index DESC),
    INDEX idx_hotspot_rank (hotspot_rank)
);
```

### 3. **kpis_temporal** (KPIs temporels pour analytics)
```sql
CREATE TABLE kpis_temporal (
    id INT AUTO_INCREMENT PRIMARY KEY,
    period_type ENUM('hourly', 'daily', 'monthly', 'yearly'),
    period_value VARCHAR(20),
    state VARCHAR(2),
    accident_count BIGINT,
    severity_avg DOUBLE,
    trend_direction ENUM('up', 'down', 'stable'),
    seasonal_factor DOUBLE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_period (period_type, period_value),
    INDEX idx_state_period (state, period_type)
);
```

### 4. **hotspots** (Zones dangereuses pour géolocalisation)
```sql
CREATE TABLE hotspots (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(2) NOT NULL,
    city VARCHAR(100),
    latitude DOUBLE,
    longitude DOUBLE,
    accident_count BIGINT,
    severity_avg DOUBLE,
    danger_score DOUBLE,
    radius_miles DOUBLE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_location (state, city),
    INDEX idx_coordinates (latitude, longitude),
    INDEX idx_danger_score (danger_score DESC)
);
```

## 🚀 Utilisation

### Exécution Complète du Pipeline

```python
from src.applications.datamart.datamart_app import DatamartApp

# Initialisation
app = DatamartApp()

# Exécution complète
result = app.run(force_refresh=False)

print(f"Pipeline terminé en {result['pipeline_duration_seconds']:.2f}s")
print(f"Tables exportées: {result['export_summary']['tables_created']}")
print(f"Lignes exportées: {result['export_summary']['total_rows_exported']}")
```

### Mise à Jour Incrémentale

```python
# Mise à jour depuis un timestamp
result = app.run_incremental_update("2024-01-01T00:00:00")
print(f"Mise à jour incrémentale: {result['total_rows_exported']} lignes")
```

### Exécution en Ligne de Commande

```bash
# Pipeline complet
python -m src.applications.datamart.datamart_app

# Avec refresh forcé
python -m src.applications.datamart.datamart_app --force-refresh

# Mise à jour incrémentale
python -m src.applications.datamart.datamart_app --incremental "2024-01-01T00:00:00"
```

## 📊 Métriques et KPIs Calculés

### KPIs de Sécurité
- **Taux d'accidents par 100k habitants** par état/ville
- **Index de dangerosité** basé sur sévérité moyenne
- **Distribution de sévérité** par conditions météo
- **Ranking des hotspots** (top 50 zones dangereuses)

### KPIs Temporels
- **Évolution mensuelle/annuelle** des accidents
- **Pics horaires** par jour de semaine
- **Facteurs saisonniers** par type météo
- **Détection de tendances** (hausse/baisse/stable)

### KPIs Infrastructure
- **Efficacité des équipements** (feux, stops, ronds-points)
- **Taux de réduction d'accidents** par type d'infrastructure
- **Score d'amélioration sécurité** par état
- **Recommandations automatiques** d'optimisation

### KPIs Performance ML
- **Métriques modèles** (accuracy, precision, recall, F1-score)
- **Importance des features** par modèle
- **Détection de drift** des données
- **Recommandations de re-entraînement**

## ⚡ Optimisations Spark

### Optimisations de Lecture
```python
# Broadcast joins pour tables de référence
broadcast_df = broadcast(small_reference_table)
result = large_df.join(broadcast_df, "key")

# Cache des DataFrames réutilisés
df.cache()
df.count()  # Déclenche la mise en cache
```

### Optimisations d'Écriture
```python
# Coalesce avant export MySQL
df.coalesce(1).write.mode("overwrite").parquet(path)

# Partitioning pour lecture optimisée
df.write.partitionBy("state", "year").parquet(path)
```

## 🗄️ Optimisations MySQL

### Index Composites Intelligents
```sql
-- Pour requêtes API /hotspots
CREATE INDEX idx_hotspots_api ON hotspots (danger_score DESC, state, city);

-- Pour requêtes API /analytics temporelles
CREATE INDEX idx_temporal_api ON kpis_temporal (period_type, state, period_value);

-- Pour requêtes API /security rankings
CREATE INDEX idx_security_api ON kpis_security (danger_index DESC, state);
```

### Partitioning par Date
```sql
-- Partitioning de la table principale par année
ALTER TABLE accidents_summary 
PARTITION BY RANGE (YEAR(accident_date)) (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

## 🔍 Analytics Avancés

### Détection d'Anomalies
- **Anomalies temporelles**: Pics/creux anormaux d'accidents
- **Anomalies géographiques**: États avec taux anormalement élevés
- **Anomalies météorologiques**: Conditions à impact exceptionnel

### Calcul de Corrélations
- **Météo vs Accidents**: Corrélation température/visibilité/accidents
- **Infrastructure vs Sécurité**: Efficacité équipements vs réduction accidents
- **Patterns temporels**: Facteurs saisonniers vs fréquence accidents

### Insights Business Automatisés
- **Top états dangereux** avec recommandations prioritaires
- **Heures de pointe** pour renforcement surveillance
- **Infrastructure optimale** à privilégier par zone
- **Corrélations significatives** pour stratégies prévention

## 🧪 Tests et Validation

### Exécution des Tests
```bash
# Tests complets
python -m pytest src/applications/datamart/test_datamart.py -v

# Tests avec couverture
python -m pytest src/applications/datamart/test_datamart.py --cov=src.applications.datamart

# Tests d'une classe spécifique
python -m pytest src/applications/datamart/test_datamart.py::TestDatamartApp -v
```

### Tests de Performance
```python
# Test de charge avec données volumineuses
def test_large_dataset_performance():
    app = DatamartApp()
    start_time = time.time()
    result = app.run()
    duration = time.time() - start_time
    
    assert duration < 300  # Moins de 5 minutes
    assert result['export_summary']['total_rows_exported'] > 10000
```

## 📈 Métriques de Performance

### Métriques Spark
- **Durée agrégations business**: < 2 minutes pour 7M+ lignes
- **Durée calcul KPIs**: < 1 minute pour toutes les dimensions
- **Utilisation mémoire**: Optimisée avec cache et broadcast joins

### Métriques MySQL
- **Durée export bulk**: < 30 secondes pour 100k+ lignes
- **Performance requêtes API**: < 100ms avec index optimisés
- **Taille tables Gold**: ~500MB pour dataset complet US-Accidents

### Métriques Qualité
- **Complétude données**: > 95% pour toutes les tables Gold
- **Cohérence KPIs**: Validation automatique des calculs
- **Fraîcheur données**: Mise à jour incrémentale < 5 minutes

## 🔧 Configuration

### Variables d'Environnement
```bash
# MySQL Gold
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=accidents_lakehouse
MYSQL_USER=tatane
MYSQL_PASSWORD=tatane
MYSQL_POOL_SIZE=10

# Hive Silver (source)
HIVE_METASTORE_URI=thrift://localhost:9083
HIVE_DATABASE=accidents_warehouse

# Spark Configuration
SPARK_MASTER=yarn
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=2
SPARK_SQL_SHUFFLE_PARTITIONS=200

# Logging
LOG_LEVEL=INFO
LOG_DATAMART_LEVEL=INFO
```

### Configuration Avancée
```yaml
# config/datamart.yaml
datamart:
  batch_size: 1000
  max_retries: 3
  retry_delay: 5
  
  kpi_thresholds:
    high_danger_threshold: 2.5
    hotspot_min_accidents: 50
    top_hotspots_count: 50
    
  optimization:
    enable_query_cache: true
    enable_partitioning: true
    index_creation: auto
```

## 🚨 Monitoring et Alertes

### Métriques à Surveiller
- **Durée pipeline**: Alerte si > 10 minutes
- **Taux d'échec**: Alerte si > 5%
- **Qualité données**: Alerte si complétude < 90%
- **Performance MySQL**: Alerte si requêtes > 1 seconde

### Logs Structurés JSON
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "application": "datamart",
  "level": "INFO",
  "message": "Pipeline completed successfully",
  "pipeline_duration_seconds": 180.5,
  "tables_exported": 6,
  "total_rows_exported": 125000,
  "data_quality_score": 96.8
}
```

## 🔄 Intégration avec l'Écosystème

### Dépendances Amont (Silver)
- **PREPROCESSOR**: Tables Hive nettoyées et enrichies
- **Données requises**: accidents_clean, weather_aggregated, infrastructure_features

### Consommateurs Aval (API)
- **FastAPI**: Endpoints optimisés avec tables Gold MySQL
- **Tableaux de bord**: Visualisations temps réel des KPIs
- **Alertes**: Notifications basées sur seuils KPIs

### Flux de Données
```
Hive Silver → DATAMART → MySQL Gold → FastAPI → Frontend
     ↓              ↓           ↓          ↓
  ORC Tables → Spark Jobs → Index Tables → Sub-second → Real-time
                                         Queries      Dashboards
```

## 🎯 Roadmap et Améliorations

### Phase 1 (Actuelle)
- ✅ Pipeline batch complet
- ✅ KPIs business essentiels
- ✅ Export MySQL optimisé
- ✅ Tests complets

### Phase 2 (Prochaine)
- 🔄 Streaming temps réel avec Kafka
- 🔄 ML automatisé pour prédictions
- 🔄 Alertes intelligentes
- 🔄 Dashboard interactif

### Phase 3 (Future)
- 📋 Multi-tenancy par région
- 📋 API GraphQL avancée
- 📋 Cache distribué Redis
- 📋 Déploiement Kubernetes

---

## 📞 Support

Pour toute question ou problème:
- **Documentation**: Voir `/docs` pour architecture détaillée
- **Tests**: Exécuter `pytest` pour validation
- **Logs**: Consulter `/var/log/lakehouse-accidents/datamart.log`
- **Monitoring**: Dashboard Grafana disponible sur port 3000

**DATAMART - Transforming Silver Data into Gold Analytics** ✨