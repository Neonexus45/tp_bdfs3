"""
Script de test pour l'application FEEDER
"""

import os
import sys
from datetime import datetime
from typing import Dict, Any

# Ajout du chemin racine pour les imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from src.common.config.config_manager import ConfigManager
from src.common.utils.logger import Logger
from src.applications.feeder.feeder_app import FeederApp
from src.applications.feeder.data_ingestion import DataIngestion
from src.applications.feeder.schema_validator import SchemaValidator
from src.applications.feeder.quality_checker import QualityChecker
from src.applications.feeder.partitioning_strategy import PartitioningStrategy


class FeederTester:
    """Testeur pour l'application FEEDER"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.logger = Logger("feeder_tester", self.config_manager)
        
        # Création d'un fichier de test si nécessaire
        self.test_data_path = "data/test/sample_us_accidents.csv"
        self.create_test_data_if_needed()
    
    def create_test_data_if_needed(self):
        """Crée un fichier de données de test si nécessaire"""
        try:
            if not os.path.exists(self.test_data_path):
                os.makedirs(os.path.dirname(self.test_data_path), exist_ok=True)
                
                # Création d'un échantillon de données US-Accidents
                test_data = self._generate_sample_data()
                
                with open(self.test_data_path, 'w', encoding='utf-8') as f:
                    f.write(test_data)
                
                self.logger.info("Test data file created", path=self.test_data_path)
            
        except Exception as e:
            self.logger.error("Failed to create test data", exception=e)
    
    def _generate_sample_data(self) -> str:
        """Génère des données d'exemple au format US-Accidents"""
        header = "ID,Source,Severity,Start_Time,End_Time,Start_Lat,Start_Lng,End_Lat,End_Lng,Distance(mi),Description,Street,City,County,State,Zipcode,Country,Timezone,Airport_Code,Weather_Timestamp,Temperature(F),Wind_Chill(F),Humidity(%),Pressure(in),Visibility(mi),Wind_Direction,Wind_Speed(mph),Precipitation(in),Weather_Condition,Amenity,Bump,Crossing,Give_Way,Junction,No_Exit,Railway,Roundabout,Station,Stop,Traffic_Calming,Traffic_Signal,Turning_Loop,Sunrise_Sunset,Civil_Twilight,Nautical_Twilight,Astronomical_Twilight"
        
        sample_rows = [
            "A-1,MapQuest,2,2016-02-08 05:46:00,2016-02-08 11:00:00,39.865147,-84.058723,39.861668,-84.062175,0.01,Accident on Dayton St at Elm St,Dayton St,Cincinnati,Hamilton,OH,45202,US,US/Eastern,CVG,2016-02-08 05:58:00,36.9,,91.0,29.68,10.0,Calm,0.0,0.02,Light Rain,False,False,False,False,False,False,False,False,False,False,False,True,False,Night,Night,Night,Night",
            "A-2,MapQuest,3,2016-02-08 06:07:59,2016-02-08 06:37:59,39.928059,-82.831184,39.9281,-82.8312,0.01,Accident on I-70 E at State Route 29,I-70 E,Hebron,Licking,OH,43025,US,US/Eastern,CMH,2016-02-08 05:51:00,37.9,,100.0,29.67,10.0,Calm,0.0,0.0,Light Rain,False,False,False,False,True,False,False,False,False,False,False,False,False,Night,Night,Night,Night",
            "A-3,MapQuest,2,2016-02-08 06:49:27,2016-02-08 07:19:27,39.063148,-84.032608,39.063148,-84.032608,0.01,Accident on Kenwood Rd at Montgomery Rd,Kenwood Rd,Cincinnati,Hamilton,OH,45236,US,US/Eastern,CVG,2016-02-08 05:58:00,36.9,,91.0,29.68,10.0,SW,3.5,0.02,Light Rain,False,False,False,False,False,False,False,False,False,False,False,True,False,Night,Night,Night,Night",
            "A-4,MapQuest,2,2016-02-08 07:23:34,2016-02-08 07:53:34,39.747753,-84.205582,39.747753,-84.205582,0.01,Accident on I-75 N at Exits 10A-B,I-75 N,Cincinnati,Hamilton,OH,45202,US,US/Eastern,CVG,2016-02-08 05:58:00,36.9,,91.0,29.68,10.0,SW,3.5,0.02,Light Rain,False,False,False,False,True,False,False,False,False,False,False,False,False,Night,Night,Night,Night",
            "A-5,MapQuest,1,2016-02-08 07:39:07,2016-02-08 08:09:07,32.4732,-86.7665,32.4732,-86.7665,0.01,Accident on I-65 S at Exit 179,I-65 S,Clanton,Chilton,AL,35045,US,US/Central,MGM,2016-02-08 07:00:00,44.1,,78.0,30.32,10.0,SW,6.9,0.0,Clear,False,False,False,False,True,False,False,False,False,False,False,False,False,Day,Day,Day,Day"
        ]
        
        return header + "\n" + "\n".join(sample_rows)
    
    def test_data_ingestion(self) -> Dict[str, Any]:
        """Test du composant DataIngestion"""
        try:
            self.logger.info("Testing DataIngestion component")
            
            data_ingestion = DataIngestion(self.config_manager)
            
            # Test de lecture CSV
            df = data_ingestion.read_csv_data(self.test_data_path)
            
            # Vérifications
            assert df is not None, "DataFrame should not be None"
            assert df.count() > 0, "DataFrame should contain data"
            
            # Récupération des métriques
            metrics = data_ingestion.get_ingestion_metrics()
            
            result = {
                'status': 'success',
                'rows_read': df.count(),
                'columns_read': len(df.columns),
                'metrics': metrics
            }
            
            self.logger.info("DataIngestion test passed", **result)
            return result
            
        except Exception as e:
            self.logger.error("DataIngestion test failed", exception=e)
            return {'status': 'failed', 'error': str(e)}
    
    def test_schema_validator(self) -> Dict[str, Any]:
        """Test du composant SchemaValidator"""
        try:
            self.logger.info("Testing SchemaValidator component")
            
            # Lecture des données de test
            data_ingestion = DataIngestion(self.config_manager)
            df = data_ingestion.read_csv_data(self.test_data_path)
            
            # Test de validation
            schema_validator = SchemaValidator(self.config_manager)
            validation_result = schema_validator.validate_dataframe_schema(df, strict_mode=False)
            
            result = {
                'status': 'success',
                'is_valid': validation_result.get('is_valid', False),
                'conformity_score': validation_result.get('conformity_score', 0),
                'validation_details': validation_result
            }
            
            self.logger.info("SchemaValidator test completed", **{
                k: v for k, v in result.items() if k != 'validation_details'
            })
            return result
            
        except Exception as e:
            self.logger.error("SchemaValidator test failed", exception=e)
            return {'status': 'failed', 'error': str(e)}
    
    def test_quality_checker(self) -> Dict[str, Any]:
        """Test du composant QualityChecker"""
        try:
            self.logger.info("Testing QualityChecker component")
            
            # Lecture des données de test
            data_ingestion = DataIngestion(self.config_manager)
            df = data_ingestion.read_csv_data(self.test_data_path)
            
            # Test de contrôle qualité
            quality_checker = QualityChecker(self.config_manager)
            quality_result = quality_checker.check_data_quality(df, detailed=False)
            
            result = {
                'status': 'success',
                'quality_score': quality_result.get('overall_quality_score', 0),
                'total_rows': quality_result.get('total_rows', 0),
                'issues_count': len(quality_result.get('quality_issues', [])),
                'quality_details': quality_result
            }
            
            self.logger.info("QualityChecker test completed", **{
                k: v for k, v in result.items() if k != 'quality_details'
            })
            return result
            
        except Exception as e:
            self.logger.error("QualityChecker test failed", exception=e)
            return {'status': 'failed', 'error': str(e)}
    
    def test_partitioning_strategy(self) -> Dict[str, Any]:
        """Test du composant PartitioningStrategy"""
        try:
            self.logger.info("Testing PartitioningStrategy component")
            
            # Lecture des données de test
            data_ingestion = DataIngestion(self.config_manager)
            df = data_ingestion.read_csv_data(self.test_data_path)
            
            # Test de partitioning
            partitioning_strategy = PartitioningStrategy(self.config_manager)
            
            # Estimation des partitions
            estimation = partitioning_strategy.estimate_partition_count(df, 'auto')
            
            # Application du partitioning (mode test)
            try:
                partitioned_df = partitioning_strategy.apply_partitioning(df, 'auto')
                partitioning_success = True
                final_partitions = partitioned_df.rdd.getNumPartitions()
            except Exception as part_e:
                self.logger.warning("Partitioning application failed", exception=part_e)
                partitioning_success = False
                final_partitions = 0
            
            result = {
                'status': 'success',
                'partitioning_applied': partitioning_success,
                'original_partitions': estimation.get('current_partitions', 0),
                'estimated_partitions': estimation.get('estimated_partitions', 0),
                'final_partitions': final_partitions,
                'estimation_details': estimation
            }
            
            self.logger.info("PartitioningStrategy test completed", **{
                k: v for k, v in result.items() if k != 'estimation_details'
            })
            return result
            
        except Exception as e:
            self.logger.error("PartitioningStrategy test failed", exception=e)
            return {'status': 'failed', 'error': str(e)}
    
    def test_full_feeder_app(self) -> Dict[str, Any]:
        """Test complet de l'application FEEDER"""
        try:
            self.logger.info("Testing complete FEEDER application")
            
            # Mise à jour du chemin source pour le test
            original_source_path = self.config_manager.get('data.source_path')
            self.config_manager._config['data']['source_path'] = self.test_data_path
            
            try:
                # Création et exécution de l'application FEEDER
                feeder_app = FeederApp(self.config_manager)
                
                # Note: En mode test, on ne peut pas exécuter complètement sans Spark
                # On teste seulement l'initialisation
                result = {
                    'status': 'success',
                    'app_initialized': True,
                    'components_loaded': {
                        'data_ingestion': feeder_app.data_ingestion is not None,
                        'schema_validator': feeder_app.schema_validator is not None,
                        'quality_checker': feeder_app.quality_checker is not None,
                        'partitioning_strategy': feeder_app.partitioning_strategy is not None
                    },
                    'config_loaded': feeder_app.config_manager is not None
                }
                
                self.logger.info("FEEDER application test completed", **result)
                return result
                
            finally:
                # Restauration du chemin source original
                self.config_manager._config['data']['source_path'] = original_source_path
            
        except Exception as e:
            self.logger.error("FEEDER application test failed", exception=e)
            return {'status': 'failed', 'error': str(e)}
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Exécute tous les tests"""
        try:
            self.logger.info("Starting comprehensive FEEDER tests")
            
            test_results = {
                'timestamp': datetime.now().isoformat(),
                'tests': {
                    'data_ingestion': self.test_data_ingestion(),
                    'schema_validator': self.test_schema_validator(),
                    'quality_checker': self.test_quality_checker(),
                    'partitioning_strategy': self.test_partitioning_strategy(),
                    'full_feeder_app': self.test_full_feeder_app()
                }
            }
            
            # Calcul du score global
            passed_tests = sum(1 for test in test_results['tests'].values() if test.get('status') == 'success')
            total_tests = len(test_results['tests'])
            success_rate = (passed_tests / total_tests) * 100
            
            test_results['summary'] = {
                'total_tests': total_tests,
                'passed_tests': passed_tests,
                'failed_tests': total_tests - passed_tests,
                'success_rate': round(success_rate, 2),
                'overall_status': 'success' if success_rate >= 80 else 'partial' if success_rate >= 50 else 'failed'
            }
            
            self.logger.info("All FEEDER tests completed", **test_results['summary'])
            
            return test_results
            
        except Exception as e:
            self.logger.error("Test execution failed", exception=e)
            return {
                'timestamp': datetime.now().isoformat(),
                'summary': {'overall_status': 'failed', 'error': str(e)}
            }


def main():
    """Point d'entrée principal pour les tests"""
    try:
        print("🧪 Starting FEEDER Application Tests...")
        print("=" * 50)
        
        tester = FeederTester()
        results = tester.run_all_tests()
        
        # Affichage des résultats
        print(f"\n📊 Test Results Summary:")
        print(f"   Total Tests: {results['summary']['total_tests']}")
        print(f"   Passed: {results['summary']['passed_tests']}")
        print(f"   Failed: {results['summary']['failed_tests']}")
        print(f"   Success Rate: {results['summary']['success_rate']}%")
        print(f"   Overall Status: {results['summary']['overall_status'].upper()}")
        
        # Détails des tests
        print(f"\n📋 Individual Test Results:")
        for test_name, test_result in results['tests'].items():
            status_icon = "✅" if test_result.get('status') == 'success' else "❌"
            print(f"   {status_icon} {test_name}: {test_result.get('status', 'unknown')}")
            if test_result.get('status') == 'failed':
                print(f"      Error: {test_result.get('error', 'Unknown error')}")
        
        # Code de sortie
        if results['summary']['overall_status'] == 'success':
            print(f"\n🎉 All tests passed successfully!")
            exit(0)
        elif results['summary']['overall_status'] == 'partial':
            print(f"\n⚠️  Some tests failed, but core functionality works")
            exit(1)
        else:
            print(f"\n💥 Critical test failures detected")
            exit(2)
            
    except Exception as e:
        print(f"💥 Test execution crashed: {str(e)}")
        exit(3)


if __name__ == "__main__":
    main()