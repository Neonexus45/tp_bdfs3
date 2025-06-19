# 🏗️ Lakehouse Infrastructure Docker Setup

## 📋 Overview

This Docker setup provides a complete **Big Data Lakehouse** infrastructure optimized for external MySQL integration:

- **Hadoop HDFS** (Distributed Storage)
- **Apache Spark** (Processing Engine) 
- **Apache Hive** (Data Warehouse)
- **MLflow** (ML Lifecycle Management)
- **External MySQL** (Metastore & Tracking)

## 🚀 Quick Start

### 1. Configure MySQL Connection
Edit [`docker/.env`](docker/.env) with your MySQL server details:
```bash
MYSQL_HOST=your-mysql-server.com
MYSQL_PORT=3306
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
```

### 2. Setup MySQL Schemas
Run the SQL script on your MySQL server:
```bash
mysql -h your-mysql-server.com -u your_username -p < scripts/setup-mysql-tables.sql
```

### 3. Start Infrastructure
```bash
# Windows
scripts\start-cluster.bat

# Linux/Mac
./scripts/start-cluster.sh
```

## 🌐 Web Interfaces

| Service | URL | Description |
|---------|-----|-------------|
| **HDFS NameNode** | http://localhost:9870 | HDFS Management |
| **Yarn ResourceManager** | http://localhost:8088 | Cluster Resources |
| **Spark Master** | http://localhost:8080 | Spark Cluster |
| **Spark Worker** | http://localhost:8081 | Worker Status |
| **HiveServer2** | http://localhost:10002 | Hive Web UI |
| **MLflow** | http://localhost:5000 | ML Experiments |

## 📁 Lakehouse Architecture

```
HDFS Structure:
/data/
├── bronze/     # Raw ingested data
├── silver/     # Cleaned & validated data  
└── gold/       # Business-ready analytics

/user/hive/warehouse/  # Hive managed tables
```

## 🔧 Architecture Details

### Optimized Design
- **bde2020 Images**: Production-ready Hadoop ecosystem
- **Environment-based Config**: Minimal configuration files
- **External MySQL**: Shared metastore and tracking
- **Volume Persistence**: Data survives container restarts

### Services
1. **namenode** - HDFS master node
2. **datanode** - HDFS storage node  
3. **resourcemanager** - Yarn cluster manager
4. **nodemanager** - Yarn worker node
5. **spark-master** - Spark cluster master
6. **spark-worker** - Spark executor
7. **hive-metastore** - Metadata service
8. **hiveserver2** - SQL interface
9. **mlflow** - ML experiment tracking

## 🛠️ Management Commands

```bash
# Health check
./scripts/health-check.sh

# Stop infrastructure  
./scripts/stop-cluster.sh    # Linux/Mac
scripts\stop-cluster.bat     # Windows

# View logs
docker-compose logs -f [service_name]
```

## 📊 Data Processing

The infrastructure is ready for your applications in [`../src/applications/`](../src/applications/):

- **Feeder**: Data ingestion to Bronze layer
- **Preprocessor**: Bronze → Silver transformation  
- **Datamart**: Silver → Gold analytics

## 🔍 Troubleshooting

### Common Issues
- **MySQL Connection**: Verify `.env` configuration
- **Port Conflicts**: Check if ports 5000, 8080, 8088, 9870 are available
- **Memory**: Ensure Docker has at least 8GB RAM allocated

### Health Check
```bash
# Check all services
docker-compose ps

# Test HDFS
docker exec namenode hdfs dfsadmin -report

# Test Hive connection
docker exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000"
```

## 📈 Production Notes

- **Security**: Configure authentication for production use
- **Scaling**: Add more workers by scaling services
- **Monitoring**: Integrate with your monitoring stack
- **Backup**: Regular HDFS and MySQL backups recommended

---
*Infrastructure optimized with bde2020 images for reliability and performance*