-- ==================== CRÉATION BASES DE DONNÉES ====================
CREATE DATABASE IF NOT EXISTS accidents_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS hive_metastore CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS mlflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ==================== TABLES GOLD LAYER (accidents_db) ====================
USE accidents_db;

-- Table principale accidents_summary
CREATE TABLE IF NOT EXISTS accidents_summary (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_state_date (state, accident_date),
    INDEX idx_severity_weather (severity, weather_category),
    INDEX idx_location (state, city),
    INDEX idx_temporal (accident_date, accident_hour)
);

-- Table KPIs sécurité
CREATE TABLE IF NOT EXISTS kpis_security (
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

-- Table KPIs temporels
CREATE TABLE IF NOT EXISTS kpis_temporal (
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

-- Table KPIs infrastructure
CREATE TABLE IF NOT EXISTS kpis_infrastructure (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(2) NOT NULL,
    infrastructure_type VARCHAR(50),
    equipment_effectiveness DOUBLE,
    accident_reduction_rate DOUBLE,
    safety_improvement_score DOUBLE,
    recommendation TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_state_infra (state, infrastructure_type),
    INDEX idx_effectiveness (equipment_effectiveness DESC)
);

-- Table hotspots
CREATE TABLE IF NOT EXISTS hotspots (
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

-- Table performance ML
CREATE TABLE IF NOT EXISTS ml_model_performance (
    id INT AUTO_INCREMENT PRIMARY KEY,
    model_name VARCHAR(100),
    model_version VARCHAR(20),
    accuracy DOUBLE,
    precision_score DOUBLE,
    recall_score DOUBLE,
    f1_score DOUBLE,
    feature_importance JSON,
    training_date TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_model (model_name, model_version),
    INDEX idx_performance (f1_score DESC)
);

-- Table métriques temps réel
CREATE TABLE IF NOT EXISTS realtime_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value DOUBLE,
    metric_unit VARCHAR(20),
    state VARCHAR(2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_metric_time (metric_name, timestamp),
    INDEX idx_state_time (state, timestamp)
);

-- Table alertes
CREATE TABLE IF NOT EXISTS alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alert_type ENUM('hotspot', 'trend', 'anomaly', 'safety'),
    severity ENUM('low', 'medium', 'high', 'critical'),
    title VARCHAR(200),
    description TEXT,
    state VARCHAR(2),
    city VARCHAR(100),
    latitude DOUBLE,
    longitude DOUBLE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP NULL,
    INDEX idx_active_alerts (is_active, severity),
    INDEX idx_location_alerts (state, city),
    INDEX idx_time_alerts (created_at DESC)
);

-- ==================== VUES POUR REPORTING ====================

-- Vue agrégée par état
CREATE OR REPLACE VIEW state_summary AS
SELECT 
    state,
    COUNT(*) as total_accidents,
    AVG(severity) as avg_severity,
    AVG(safety_score) as avg_safety_score,
    COUNT(DISTINCT city) as cities_count,
    MIN(accident_date) as first_accident,
    MAX(accident_date) as last_accident
FROM accidents_summary 
GROUP BY state;

-- Vue hotspots actifs
CREATE OR REPLACE VIEW active_hotspots AS
SELECT 
    h.*,
    a.total_accidents,
    a.avg_severity
FROM hotspots h
JOIN (
    SELECT state, city, COUNT(*) as total_accidents, AVG(severity) as avg_severity
    FROM accidents_summary 
    GROUP BY state, city
) a ON h.state = a.state AND h.city = a.city
WHERE h.danger_score > 0.7
ORDER BY h.danger_score DESC;

-- Vue tendances mensuelles
CREATE OR REPLACE VIEW monthly_trends AS
SELECT 
    DATE_FORMAT(accident_date, '%Y-%m') as month,
    state,
    COUNT(*) as accident_count,
    AVG(severity) as avg_severity,
    COUNT(DISTINCT city) as affected_cities
FROM accidents_summary 
WHERE accident_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
GROUP BY DATE_FORMAT(accident_date, '%Y-%m'), state
ORDER BY month DESC, state;

COMMIT;

-- ==================== DONNÉES DE TEST ====================
-- Insertion de quelques données de test pour validation
INSERT IGNORE INTO accidents_summary (id, state, city, severity, accident_date, accident_hour, weather_category, temperature_category, infrastructure_count, safety_score, distance_miles) VALUES
('TEST001', 'CA', 'Los Angeles', 3, '2024-01-15', 14, 'Clear', 'Mild', 5, 0.75, 2.5),
('TEST002', 'NY', 'New York', 2, '2024-01-16', 8, 'Rain', 'Cold', 8, 0.85, 1.2),
('TEST003', 'TX', 'Houston', 4, '2024-01-17', 22, 'Fog', 'Hot', 3, 0.45, 5.8);

INSERT IGNORE INTO kpis_security (state, city, accident_rate_per_100k, danger_index, severity_distribution, hotspot_rank) VALUES
('CA', 'Los Angeles', 125.5, 0.78, '{"1": 20, "2": 35, "3": 30, "4": 15}', 1),
('NY', 'New York', 98.2, 0.65, '{"1": 25, "2": 40, "3": 25, "4": 10}', 2),
('TX', 'Houston', 156.8, 0.82, '{"1": 15, "2": 30, "3": 35, "4": 20}', 3);

COMMIT;