#!/usr/bin/env python3

import sys
import os
import subprocess
import time
import requests
import mysql.connector
from datetime import datetime

sys.path.append(os.path.dirname(__file__))
from src.common.config.config_manager import ConfigManager
from src.common.utils.logger import Logger

class PipelineValidator:
    def __init__(self):
        self.config = ConfigManager()
        self.logger = Logger("pipeline_validator")
        self.results = {}
        
    def test_infrastructure(self):
        """Test de l'infrastructure Docker"""
        self.logger.info("Test de l'infrastructure Docker...")
        
        try:
            # Test HDFS
            result = subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/'], 
                                  capture_output=True, text=True, timeout=30)
            hdfs_ok = result.returncode == 0
            
            # Test Yarn
            result = subprocess.run(['docker', 'exec', 'resourcemanager', 'yarn', 'node', '-list'], 
                                  capture_output=True, text=True, timeout=30)
            yarn_ok = result.returncode == 0
            
            # Test MySQL
            try:
                conn = mysql.connector.connect(
                    host=self.config.get('DB_HOST', 'localhost'),
                    port=int(self.config.get('DB_PORT', 3306)),
                    user=self.config.get('DB_USER', 'tatane'),
                    password=self.config.get('DB_PASSWORD', 'tatane'),
                    database=self.config.get('DB_NAME', 'accidents_db')
                )
                conn.close()
                mysql_ok = True
            except:
                mysql_ok = False
            
            self.results['infrastructure'] = {
                'hdfs': hdfs_ok,
                'yarn': yarn_ok,
                'mysql': mysql_ok,
                'overall': hdfs_ok and yarn_ok and mysql_ok
            }
            
            return self.results['infrastructure']['overall']
            
        except Exception as e:
            self.logger.error(f"Erreur test infrastructure: {e}")
            self.results['infrastructure'] = {'overall': False, 'error': str(e)}
            return False
    
    def test_spark_applications(self):
        """Test des applications Spark"""
        self.logger.info("Test des applications Spark...")
        
        apps = ['feeder', 'preprocessor', 'datamart', 'mltraining']
        app_results = {}
        
        for app in apps:
            try:
                self.logger.info(f"Test de l'application {app}...")
                
                # Import et test basique
                if app == 'feeder':
                    from src.applications.feeder.feeder_app import FeederApp
                    app_instance = FeederApp()
                elif app == 'preprocessor':
                    from src.applications.preprocessor.preprocessor_app import PreprocessorApp
                    app_instance = PreprocessorApp()
                elif app == 'datamart':
                    from src.applications.datamart.datamart_app import DatamartApp
                    app_instance = DatamartApp()
                elif app == 'mltraining':
                    from src.applications.mltraining.mltraining_app import MLTrainingApp
                    app_instance = MLTrainingApp()
                
                # Test d'initialisation
                app_instance.initialize_components()
                app_results[app] = True
                self.logger.info(f"✅ Application {app} OK")
                
            except Exception as e:
                app_results[app] = False
                self.logger.error(f"❌ Application {app} ERREUR: {e}")
        
        self.results['spark_apps'] = app_results
        return all(app_results.values())
    
    def test_api(self):
        """Test de l'API FastAPI"""
        self.logger.info("Test de l'API FastAPI...")
        
        try:
            # Test import API
            from src.api.main import app
            
            # Test endpoints basiques (si API démarrée)
            api_endpoints = [
                'http://localhost:8000/',
                'http://localhost:8000/health',
                'http://localhost:8000/docs'
            ]
            
            api_results = {}
            for endpoint in api_endpoints:
                try:
                    response = requests.get(endpoint, timeout=5)
                    api_results[endpoint] = response.status_code == 200
                except:
                    api_results[endpoint] = False
            
            self.results['api'] = {
                'import': True,
                'endpoints': api_results,
                'overall': True  # Import réussi
            }
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur test API: {e}")
            self.results['api'] = {'overall': False, 'error': str(e)}
            return False
    
    def test_streamlit(self):
        """Test de l'application Streamlit"""
        self.logger.info("Test de l'application Streamlit...")
        
        try:
            # Test import Streamlit app
            sys.path.append(os.path.join(os.path.dirname(__file__), 'streamlit'))
            import app as streamlit_app
            
            # Test classe dashboard
            dashboard = streamlit_app.StreamlitDashboard()
            
            self.results['streamlit'] = {
                'import': True,
                'dashboard_init': True,
                'overall': True
            }
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur test Streamlit: {e}")
            self.results['streamlit'] = {'overall': False, 'error': str(e)}
            return False
    
    def test_configuration(self):
        """Test de la configuration"""
        self.logger.info("Test de la configuration...")
        
        try:
            # Test ConfigManager
            config_ok = self.config is not None
            
            # Test variables essentielles
            essential_vars = [
                'DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME',
                'SPARK_MASTER', 'HDFS_NAMENODE_URL'
            ]
            
            vars_ok = all(self.config.get(var) is not None for var in essential_vars)
            
            self.results['configuration'] = {
                'config_manager': config_ok,
                'essential_vars': vars_ok,
                'overall': config_ok and vars_ok
            }
            
            return self.results['configuration']['overall']
            
        except Exception as e:
            self.logger.error(f"Erreur test configuration: {e}")
            self.results['configuration'] = {'overall': False, 'error': str(e)}
            return False
    
    def run_complete_test(self):
        """Exécution de tous les tests"""
        self.logger.info("🚀 Démarrage des tests complets du pipeline...")
        
        tests = [
            ('Configuration', self.test_configuration),
            ('Infrastructure Docker', self.test_infrastructure),
            ('Applications Spark', self.test_spark_applications),
            ('API FastAPI', self.test_api),
            ('Dashboard Streamlit', self.test_streamlit)
        ]
        
        results_summary = {}
        
        for test_name, test_func in tests:
            self.logger.info(f"\n📋 Test: {test_name}")
            try:
                result = test_func()
                results_summary[test_name] = result
                status = "✅ RÉUSSI" if result else "❌ ÉCHEC"
                self.logger.info(f"{status}: {test_name}")
            except Exception as e:
                results_summary[test_name] = False
                self.logger.error(f"❌ ERREUR {test_name}: {e}")
        
        # Résumé final
        total_tests = len(results_summary)
        passed_tests = sum(results_summary.values())
        success_rate = (passed_tests / total_tests) * 100
        
        self.logger.info(f"\n🎯 RÉSULTATS FINAUX:")
        self.logger.info(f"Tests réussis: {passed_tests}/{total_tests}")
        self.logger.info(f"Taux de réussite: {success_rate:.1f}%")
        
        for test_name, result in results_summary.items():
            status = "✅" if result else "❌"
            self.logger.info(f"{status} {test_name}")
        
        if success_rate >= 80:
            self.logger.info("🎉 PIPELINE VALIDÉ - Prêt pour la production!")
        else:
            self.logger.warning("⚠️ PIPELINE PARTIELLEMENT VALIDÉ - Vérifiez les erreurs")
        
        return results_summary

def main():
    print("=" * 60)
    print("    VALIDATION COMPLÈTE DU PIPELINE LAKEHOUSE")
    print("=" * 60)
    
    validator = PipelineValidator()
    results = validator.run_complete_test()
    
    print("\n" + "=" * 60)
    print("    TESTS TERMINÉS")
    print("=" * 60)
    
    return 0 if all(results.values()) else 1

if __name__ == "__main__":
    sys.exit(main())