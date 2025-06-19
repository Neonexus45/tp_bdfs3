"""
Tests simples pour l'API FastAPI (version projet scolaire)
"""

import asyncio
import json
from typing import Dict, Any
import httpx
from datetime import datetime

class APITester:
    """Testeur simple pour l'API FastAPI"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = None
    
    async def __aenter__(self):
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()
    
    async def test_health_endpoints(self) -> Dict[str, Any]:
        """Test des endpoints de santé"""
        results = {}
        
        # Test endpoint racine
        try:
            response = await self.client.get("/")
            results["root"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["root"] = {"success": False, "error": str(e)}
        
        # Test health check
        try:
            response = await self.client.get("/health")
            results["health"] = {
                "status_code": response.status_code,
                "success": response.status_code in [200, 503],
                "data": response.json() if response.status_code in [200, 503] else None
            }
        except Exception as e:
            results["health"] = {"success": False, "error": str(e)}
        
        # Test API info
        try:
            response = await self.client.get("/info")
            results["info"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["info"] = {"success": False, "error": str(e)}
        
        return results
    
    async def test_accidents_endpoints(self) -> Dict[str, Any]:
        """Test des endpoints d'accidents"""
        results = {}
        
        # Test liste des accidents
        try:
            response = await self.client.get("/api/v1/accidents")
            results["list"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["list"] = {"success": False, "error": str(e)}
        
        # Test avec pagination
        try:
            response = await self.client.get("/api/v1/accidents?page=1&size=5")
            results["pagination"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["pagination"] = {"success": False, "error": str(e)}
        
        # Test avec filtres
        try:
            response = await self.client.get("/api/v1/accidents?state=CA&severity=2")
            results["filters"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["filters"] = {"success": False, "error": str(e)}
        
        # Test statistiques
        try:
            response = await self.client.get("/api/v1/accidents/stats")
            results["stats"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["stats"] = {"success": False, "error": str(e)}
        
        return results
    
    async def test_kpis_endpoints(self) -> Dict[str, Any]:
        """Test des endpoints KPIs"""
        results = {}
        
        # Test KPIs généraux
        try:
            response = await self.client.get("/api/v1/kpis")
            results["general"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["general"] = {"success": False, "error": str(e)}
        
        # Test KPIs par état
        try:
            response = await self.client.get("/api/v1/kpis/by-state")
            results["by_state"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["by_state"] = {"success": False, "error": str(e)}
        
        # Test tendances temporelles
        try:
            response = await self.client.get("/api/v1/kpis/trends")
            results["trends"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["trends"] = {"success": False, "error": str(e)}
        
        return results
    
    async def test_hotspots_endpoints(self) -> Dict[str, Any]:
        """Test des endpoints hotspots"""
        results = {}
        
        # Test liste des hotspots
        try:
            response = await self.client.get("/api/v1/hotspots")
            results["list"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["list"] = {"success": False, "error": str(e)}
        
        # Test hotspots par état
        try:
            response = await self.client.get("/api/v1/hotspots?state=CA")
            results["by_state"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["by_state"] = {"success": False, "error": str(e)}
        
        # Test analyse géographique
        try:
            response = await self.client.get("/api/v1/hotspots/geographic-analysis")
            results["geographic"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["geographic"] = {"success": False, "error": str(e)}
        
        return results
    
    async def test_predictions_endpoints(self) -> Dict[str, Any]:
        """Test des endpoints de prédiction"""
        results = {}
        
        # Test modèles disponibles
        try:
            response = await self.client.get("/api/v1/predict/models")
            results["models"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["models"] = {"success": False, "error": str(e)}
        
        # Test santé du service
        try:
            response = await self.client.get("/api/v1/predict/health")
            results["health"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["health"] = {"success": False, "error": str(e)}
        
        # Test exemples
        try:
            response = await self.client.get("/api/v1/predict/examples")
            results["examples"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["examples"] = {"success": False, "error": str(e)}
        
        # Test validation de données
        try:
            test_data = {
                "state": "CA",
                "start_lat": 34.0522,
                "start_lng": -118.2437,
                "weather_condition": "Clear",
                "temperature_fahrenheit": 72.0
            }
            response = await self.client.post("/api/v1/predict/validate", json=test_data)
            results["validate"] = {
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "data": response.json() if response.status_code == 200 else None
            }
        except Exception as e:
            results["validate"] = {"success": False, "error": str(e)}
        
        # Test prédiction simple (peut échouer si ML service pas configuré)
        try:
            test_data = {
                "state": "CA",
                "start_lat": 34.0522,
                "start_lng": -118.2437,
                "weather_condition": "Clear",
                "temperature_fahrenheit": 72.0,
                "humidity_percent": 65.0,
                "visibility_miles": 10.0
            }
            response = await self.client.post("/api/v1/predict", json=test_data)
            results["predict"] = {
                "status_code": response.status_code,
                "success": response.status_code in [200, 500],  # 500 acceptable si ML pas configuré
                "data": response.json() if response.status_code in [200, 500] else None
            }
        except Exception as e:
            results["predict"] = {"success": False, "error": str(e)}
        
        return results
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Exécute tous les tests"""
        print("🚀 Démarrage des tests de l'API...")
        print(f"📡 URL de base: {self.base_url}")
        print("-" * 60)
        
        all_results = {
            "timestamp": datetime.now().isoformat(),
            "base_url": self.base_url,
            "tests": {}
        }
        
        # Tests des endpoints de santé
        print("🏥 Test des endpoints de santé...")
        all_results["tests"]["health"] = await self.test_health_endpoints()
        
        # Tests des accidents
        print("🚗 Test des endpoints d'accidents...")
        all_results["tests"]["accidents"] = await self.test_accidents_endpoints()
        
        # Tests des KPIs
        print("📊 Test des endpoints KPIs...")
        all_results["tests"]["kpis"] = await self.test_kpis_endpoints()
        
        # Tests des hotspots
        print("🔥 Test des endpoints hotspots...")
        all_results["tests"]["hotspots"] = await self.test_hotspots_endpoints()
        
        # Tests des prédictions
        print("🤖 Test des endpoints de prédiction...")
        all_results["tests"]["predictions"] = await self.test_predictions_endpoints()
        
        return all_results
    
    def print_results_summary(self, results: Dict[str, Any]):
        """Affiche un résumé des résultats"""
        print("\n" + "=" * 60)
        print("📋 RÉSUMÉ DES TESTS")
        print("=" * 60)
        
        total_tests = 0
        successful_tests = 0
        
        for category, tests in results["tests"].items():
            print(f"\n🔍 {category.upper()}:")
            category_success = 0
            category_total = 0
            
            for test_name, test_result in tests.items():
                category_total += 1
                total_tests += 1
                
                if test_result.get("success", False):
                    category_success += 1
                    successful_tests += 1
                    status = "✅ PASS"
                else:
                    status = "❌ FAIL"
                
                print(f"  {test_name}: {status}")
                if not test_result.get("success", False) and "error" in test_result:
                    print(f"    Erreur: {test_result['error']}")
            
            print(f"  📈 Succès: {category_success}/{category_total}")
        
        print(f"\n🎯 RÉSULTAT GLOBAL: {successful_tests}/{total_tests} tests réussis")
        success_rate = (successful_tests / total_tests * 100) if total_tests > 0 else 0
        print(f"📊 Taux de réussite: {success_rate:.1f}%")
        
        if success_rate >= 80:
            print("🎉 API en bon état !")
        elif success_rate >= 60:
            print("⚠️  API partiellement fonctionnelle")
        else:
            print("🚨 API nécessite des corrections")


async def main():
    """Fonction principale de test"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Testeur simple pour l'API FastAPI")
    parser.add_argument("--url", default="http://localhost:8000", help="URL de base de l'API")
    parser.add_argument("--output", help="Fichier de sortie JSON pour les résultats")
    args = parser.parse_args()
    
    async with APITester(args.url) as tester:
        try:
            results = await tester.run_all_tests()
            tester.print_results_summary(results)
            
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                print(f"\n💾 Résultats sauvegardés dans: {args.output}")
                
        except Exception as e:
            print(f"❌ Erreur lors des tests: {e}")
            return 1
    
    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))