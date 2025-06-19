#!/usr/bin/env python3
"""
Script de test pour vérifier la configuration du projet lakehouse
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_imports():
    """Test des imports des modules principaux"""
    try:
        from src.common.config.config_manager import ConfigManager
        from src.common.config.spark_config import SparkConfig
        from src.common.config.database_config import DatabaseConfig
        from src.common.utils.logger import Logger
        from src.common.utils.database_connector import DatabaseConnector
        from src.common.utils.spark_utils import SparkUtils
        from src.common.utils.validation_utils import ValidationUtils
        from src.common.exceptions.custom_exceptions import LakehouseException
        
        print("✅ Tous les imports sont réussis")
        return True
    except ImportError as e:
        print(f"❌ Erreur d'import: {e}")
        return False

def test_config_manager():
    """Test du gestionnaire de configuration"""
    try:
        from src.common.config.config_manager import ConfigManager
        config = ConfigManager()
        
        mysql_config = config.get('mysql')
        spark_config = config.get('spark')
        api_config = config.get('api')
        
        assert mysql_config is not None, "Configuration MySQL manquante"
        assert spark_config is not None, "Configuration Spark manquante"
        assert api_config is not None, "Configuration API manquante"
        
        mysql_url = config.get_mysql_url()
        assert mysql_url.startswith('mysql+pymysql://'), "URL MySQL invalide"
        
        hdfs_path = config.get_hdfs_path('bronze')
        assert hdfs_path is not None, "Chemin HDFS manquant"
        
        print("✅ ConfigManager fonctionne correctement")
        return True
    except Exception as e:
        print(f"❌ Erreur ConfigManager: {e}")
        return False

def test_spark_config():
    """Test de la configuration Spark"""
    try:
        from src.common.config.config_manager import ConfigManager
        from src.common.config.spark_config import SparkConfig
        config_manager = ConfigManager()
        spark_config = SparkConfig(config_manager)
        
        feeder_config = spark_config.get_feeder_config()
        assert 'spark.app.name' in feeder_config, "Configuration Feeder manquante"
        
        preprocessor_config = spark_config.get_preprocessor_config()
        assert 'spark.app.name' in preprocessor_config, "Configuration Preprocessor manquante"
        
        print("✅ SparkConfig fonctionne correctement")
        return True
    except Exception as e:
        print(f"❌ Erreur SparkConfig: {e}")
        return False

def test_database_config():
    """Test de la configuration base de données"""
    try:
        from src.common.config.config_manager import ConfigManager
        from src.common.config.database_config import DatabaseConfig
        config_manager = ConfigManager()
        db_config = DatabaseConfig(config_manager)
        
        mysql_params = db_config.get_mysql_connection_params()
        assert 'host' in mysql_params, "Paramètres MySQL manquants"
        
        hive_params = db_config.get_hive_connection_params()
        assert 'host' in hive_params, "Paramètres Hive manquants"
        
        print("✅ DatabaseConfig fonctionne correctement")
        return True
    except Exception as e:
        print(f"❌ Erreur DatabaseConfig: {e}")
        return False

def test_logger():
    """Test du logger structuré"""
    try:
        from src.common.config.config_manager import ConfigManager
        from src.common.utils.logger import Logger
        config_manager = ConfigManager()
        logger = Logger("test_logger", config_manager)
        
        logger.info("Test message", test_param="test_value")
        logger.log_performance("test_operation", 1.5, rows_processed=1000)
        
        print("✅ Logger fonctionne correctement")
        return True
    except Exception as e:
        print(f"❌ Erreur Logger: {e}")
        return False

def test_validation_utils():
    """Test des utilitaires de validation"""
    try:
        from src.common.config.config_manager import ConfigManager
        from src.common.utils.validation_utils import ValidationUtils
        config_manager = ConfigManager()
        validator = ValidationUtils(config_manager)
        
        schema = validator.get_us_accidents_schema()
        assert len(schema.fields) == 47, f"Schéma incorrect: {len(schema.fields)} colonnes au lieu de 47"
        
        quality_rules = validator.get_default_quality_rules()
        assert 'max_null_percentage' in quality_rules, "Règles de qualité manquantes"
        
        print("✅ ValidationUtils fonctionne correctement")
        return True
    except Exception as e:
        print(f"❌ Erreur ValidationUtils: {e}")
        return False

def test_exceptions():
    """Test des exceptions personnalisées"""
    try:
        from src.common.exceptions.custom_exceptions import (
            ConfigurationError, DataValidationError, SparkJobError
        )
        
        config_error = ConfigurationError("Test config error", "test_key", "test_value")
        assert config_error.config_key == "test_key", "Exception ConfigurationError incorrecte"
        
        validation_error = DataValidationError("Test validation error", "schema", ["rule1"])
        assert validation_error.validation_type == "schema", "Exception DataValidationError incorrecte"
        
        print("✅ Exceptions personnalisées fonctionnent correctement")
        return True
    except Exception as e:
        print(f"❌ Erreur Exceptions: {e}")
        return False

def test_project_structure():
    """Test de la structure du projet"""
    try:
        required_dirs = [
            "src/common/config",
            "src/common/utils", 
            "src/common/exceptions",
            "src/applications/feeder",
            "src/applications/preprocessor",
            "src/applications/datamart",
            "src/applications/mltraining",
            "src/api/models",
            "src/api/routers",
            "src/api/middleware"
        ]
        
        for dir_path in required_dirs:
            if not Path(dir_path).exists():
                print(f"❌ Répertoire manquant: {dir_path}")
                return False
        
        required_files = [
            ".env",
            "requirements.txt",
            "setup.py",
            "src/__init__.py",
            "src/common/config/config_manager.py",
            "src/common/utils/logger.py",
            "src/api/main.py"
        ]
        
        for file_path in required_files:
            if not Path(file_path).exists():
                print(f"❌ Fichier manquant: {file_path}")
                return False
        
        print("✅ Structure du projet correcte")
        return True
    except Exception as e:
        print(f"❌ Erreur structure projet: {e}")
        return False

def main():
    """Fonction principale de test"""
    print("🚀 Test de configuration du projet lakehouse US-Accidents")
    print("=" * 60)
    
    tests = [
        ("Structure du projet", test_project_structure),
        ("Imports des modules", test_imports),
        ("ConfigManager", test_config_manager),
        ("SparkConfig", test_spark_config),
        ("DatabaseConfig", test_database_config),
        ("Logger", test_logger),
        ("ValidationUtils", test_validation_utils),
        ("Exceptions", test_exceptions),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 Test: {test_name}")
        if test_func():
            passed += 1
        else:
            print(f"   ⚠️  Le test '{test_name}' a échoué")
    
    print("\n" + "=" * 60)
    print(f"📊 Résultats: {passed}/{total} tests réussis")
    
    if passed == total:
        print("🎉 Tous les tests sont passés! Le projet est correctement configuré.")
        return 0
    else:
        print("⚠️  Certains tests ont échoué. Vérifiez la configuration.")
        return 1

if __name__ == "__main__":
    sys.exit(main())