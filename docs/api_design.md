# Design API FastAPI avec Spécifications Endpoints

## 🌐 Architecture API FastAPI

```mermaid
graph TB
    subgraph "API FastAPI - Documentation Française"
        MAIN[main.py<br/>Point d'entrée FastAPI]
        SWAGGER[Swagger UI<br/>Documentation automatique]
        REDOC[ReDoc<br/>Documentation alternative]
    end

    subgraph "Routers Métier"
        ACCIDENTS[/accidents<br/>Données accidents]
        HOTSPOTS[/hotspots<br/>Zones à risque]
        KPIS[/kpis/{metric}<br/>Indicateurs clés]
        PREDICT[POST /predict<br/>Prédictions ML]
        HEALTH[/health<br/>Santé système]
    end

    subgraph "Modèles Pydantic"
        REQUEST[Request Models<br/>Validation entrée]
        RESPONSE[Response Models<br/>Format sortie]
        BASE[Base Models<br/>Modèles communs]
    end

    subgraph "Services Métier"
        ACCIDENT_SVC[AccidentService<br/>Logique accidents]
        HOTSPOT_SVC[HotspotService<br/>Logique hotspots]
        KPI_SVC[KPIService<br/>Logique KPIs]
        PREDICTION_SVC[PredictionService<br/>Logique ML]
    end

    subgraph "Middleware & Sécurité"
        AUTH[Authentification JWT<br/>Sécurité API]
        RATE_LIMIT[Rate Limiting<br/>Protection DDoS]
        CORS[CORS<br/>Cross-Origin]
        LOGGING[Request Logging<br/>Audit trail]
    end

    subgraph "Base de Données"
        MYSQL[MySQL Gold Layer<br/>Données agrégées]
        CACHE[Redis Cache<br/>Performance]
    end

    MAIN --> ACCIDENTS
    MAIN --> HOTSPOTS
    MAIN --> KPIS
    MAIN --> PREDICT
    MAIN --> HEALTH
    MAIN --> SWAGGER
    MAIN --> REDOC
    
    ACCIDENTS --> REQUEST
    HOTSPOTS --> REQUEST
    KPIS --> REQUEST
    PREDICT --> REQUEST
    
    REQUEST --> RESPONSE
    RESPONSE --> BASE
    
    ACCIDENTS --> ACCIDENT_SVC
    HOTSPOTS --> HOTSPOT_SVC
    KPIS --> KPI_SVC
    PREDICT --> PREDICTION_SVC
    
    ACCIDENT_SVC --> MYSQL
    HOTSPOT_SVC --> MYSQL
    KPI_SVC --> MYSQL
    PREDICTION_SVC --> MYSQL
    
    MYSQL --> CACHE
    
    MAIN --> AUTH
    MAIN --> RATE_LIMIT
    MAIN --> CORS
    MAIN --> LOGGING
```

## 🚀 Configuration FastAPI Principale

### Point d'Entrée API

```python
# src/api/main.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
import time
import uvicorn

from .routers import accidents, hotspots, kpis, predictions, health
from .middleware.rate_limiting import RateLimitMiddleware
from config.config_manager import ConfigManager

# Configuration
config = ConfigManager()
api_config = config.get('api')

# Création de l'application FastAPI
app = FastAPI(
    title=api_config['title'],
    description="""
    # API d'Analyse des Accidents de la Route aux États-Unis
    
    Cette API fournit un accès aux données d'analyse des accidents de la route aux États-Unis,
    incluant les zones à risque, les patterns temporels et les prédictions de sévérité.
    
    ## Fonctionnalités Principales
    
    - **Données d'Accidents** : Accès aux données historiques avec filtrage avancé
    - **Zones à Risque** : Identification des hotspots géographiques
    - **Indicateurs Clés** : KPIs et métriques de performance
    - **Prédictions ML** : Prédiction de sévérité basée sur les conditions
    
    ## Authentification
    
    L'API est accessible sans authentification (version projet scolaire).
    
    ## Pagination
    
    Toutes les listes sont paginées avec les paramètres `page` et `taille`.
    """,
    version=api_config['version'],
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "Équipe Lakehouse Accidents US",
        "email": "support@lakehouse-accidents.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=api_config.get('cors_origins', ["*"]),
    allow_credentials=api_config.get('cors_allow_credentials', True),
    allow_methods=api_config.get('cors_allow_methods', ["*"]),
    allow_headers=api_config.get('cors_allow_headers', ["*"]),
)

# Middleware de sécurité
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])
app.add_middleware(AuthenticationMiddleware)
app.add_middleware(RateLimitMiddleware, calls=100, period=60)

# Middleware de logging des requêtes
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    
    # Log structuré de la requête
    logger.info(
        "API Request",
        extra={
            "method": request.method,
            "url": str(request.url),
            "status_code": response.status_code,
            "process_time": process_time,
            "client_ip": request.client.host,
            "user_agent": request.headers.get("user-agent"),
        }
    )
    
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Gestionnaire d'erreurs global
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "erreur": True,
            "message": exc.detail,
            "code_erreur": exc.status_code,
            "timestamp": time.time(),
            "chemin": str(request.url.path)
        }
    )

# Inclusion des routers
app.include_router(
    accidents.router,
    prefix="/api/v1/accidents",
    tags=["Accidents"],
    responses={404: {"description": "Accidents non trouvés"}}
)

app.include_router(
    hotspots.router,
    prefix="/api/v1/hotspots",
    tags=["Zones à Risque"],
    responses={404: {"description": "Hotspots non trouvés"}}
)

app.include_router(
    kpis.router,
    prefix="/api/v1/kpis",
    tags=["Indicateurs Clés"],
    responses={404: {"description": "KPIs non trouvés"}}
)

app.include_router(
    predictions.router,
    prefix="/api/v1/predictions",
    tags=["Prédictions ML"],
    responses={422: {"description": "Données d'entrée invalides"}}
)

app.include_router(
    health.router,
    prefix="/api/v1/health",
    tags=["Santé Système"],
    include_in_schema=False
)

# Point d'entrée racine
@app.get("/", tags=["Racine"])
async def racine():
    """Point d'entrée de l'API avec informations de base"""
    return {
        "message": "API Lakehouse Accidents US",
        "version": api_config['version'],
        "documentation": "/docs",
        "status": "opérationnel",
        "endpoints_principaux": {
            "accidents": "/api/v1/accidents",
            "hotspots": "/api/v1/hotspots",
            "kpis": "/api/v1/kpis",
            "predictions": "/api/v1/predictions"
        }
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=api_config['host'],
        port=api_config['port'],
        reload=config.get('debug', False),
        log_level="info"
    )
```

## 📊 Endpoints Détaillés

### 1. Router Accidents

```python
# src/api/routers/accidents.py
from fastapi import APIRouter, Depends, Query, HTTPException, Path
from typing import Optional, List
from datetime import date, datetime

from ..models.request_models import AccidentFilters, PaginationParams
from ..models.response_models import AccidentResponse, AccidentListResponse
from ..services.accident_service import AccidentService
from ..dependencies.database import get_db_session

router = APIRouter()

@router.get(
    "/",
    response_model=AccidentListResponse,
    summary="Récupérer la liste des accidents",
    description="""
    Récupère une liste paginée des accidents avec possibilité de filtrage.
    
    **Filtres disponibles :**
    - État (code à 2 lettres, ex: OH, CA)
    - Niveau de sévérité (1-4)
    - Période temporelle (date_debut et date_fin)
    - Conditions météorologiques
    - Type d'infrastructure
    
    **Pagination :**
    - Taille de page par défaut : 100
    - Taille maximale : 1000
    """
)
async def lister_accidents(
    # Filtres de recherche
    etat: Optional[str] = Query(None, description="Code état (ex: OH, CA)", regex="^[A-Z]{2}$"),
    severite: Optional[int] = Query(None, description="Niveau de sévérité (1-4)", ge=1, le=4),
    date_debut: Optional[date] = Query(None, description="Date début période (YYYY-MM-DD)"),
    date_fin: Optional[date] = Query(None, description="Date fin période (YYYY-MM-DD)"),
    condition_meteo: Optional[str] = Query(None, description="Condition météorologique"),
    infrastructure: Optional[str] = Query(None, description="Type d'infrastructure"),
    
    # Pagination
    page: int = Query(1, description="Numéro de page", ge=1),
    taille: int = Query(100, description="Taille de page", ge=1, le=1000),
    
    # Tri
    tri_par: str = Query("Start_Time", description="Champ de tri"),
    ordre: str = Query("desc", description="Ordre de tri (asc/desc)", regex="^(asc|desc)$"),
    
    # Dépendances
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Récupère une liste paginée des accidents avec filtrage"""
    
    # Validation des dates
    if date_debut and date_fin and date_debut > date_fin:
        raise HTTPException(
            status_code=422,
            detail="La date de début doit être antérieure à la date de fin"
        )
    
    # Construction des filtres
    filtres = AccidentFilters(
        etat=etat,
        severite=severite,
        date_debut=date_debut,
        date_fin=date_fin,
        condition_meteo=condition_meteo,
        infrastructure=infrastructure
    )
    
    pagination = PaginationParams(
        page=page,
        taille=taille,
        tri_par=tri_par,
        ordre=ordre
    )
    
    # Appel du service
    service = AccidentService(db_session)
    resultat = await service.lister_accidents(filtres, pagination)
    
    return resultat

@router.get(
    "/{accident_id}",
    response_model=AccidentResponse,
    summary="Récupérer un accident spécifique",
    description="Récupère les détails complets d'un accident par son ID"
)
async def obtenir_accident(
    accident_id: str = Path(..., description="ID unique de l'accident"),
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Récupère un accident spécifique par son ID"""
    
    service = AccidentService(db_session)
    accident = await service.obtenir_accident_par_id(accident_id)
    
    if not accident:
        raise HTTPException(
            status_code=404,
            detail=f"Accident avec l'ID {accident_id} non trouvé"
        )
    
    return accident

@router.get(
    "/statistiques/resume",
    summary="Statistiques résumées des accidents",
    description="Fournit un résumé statistique des accidents selon les filtres appliqués"
)
async def statistiques_accidents(
    etat: Optional[str] = Query(None, regex="^[A-Z]{2}$"),
    date_debut: Optional[date] = Query(None),
    date_fin: Optional[date] = Query(None),
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Calcule des statistiques résumées sur les accidents"""
    
    service = AccidentService(db_session)
    stats = await service.calculer_statistiques(etat, date_debut, date_fin)
    
    return {
        "total_accidents": stats["total"],
        "severite_moyenne": stats["severite_moyenne"],
        "repartition_par_severite": stats["repartition_severite"],
        "top_etats": stats["top_etats"],
        "conditions_meteo_frequentes": stats["conditions_meteo"],
        "periode_analyse": {
            "debut": date_debut,
            "fin": date_fin
        }
    }
```

### 2. Router Hotspots

```python
# src/api/routers/hotspots.py
from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional, List
from ..models.response_models import HotspotResponse, HotspotListResponse
from ..services.hotspot_service import HotspotService

router = APIRouter()

@router.get(
    "/",
    response_model=HotspotListResponse,
    summary="Récupérer les zones à risque",
    description="""
    Récupère les zones géographiques présentant un risque élevé d'accidents.
    
    **Critères de sélection :**
    - Score de risque minimum configurable
    - Rayon de recherche géographique
    - Filtrage par type d'infrastructure
    - Nombre minimum d'accidents
    """
)
async def lister_hotspots(
    # Filtres géographiques
    etat: Optional[str] = Query(None, description="Code état", regex="^[A-Z]{2}$"),
    comte: Optional[str] = Query(None, description="Nom du comté"),
    latitude: Optional[float] = Query(None, description="Latitude centre recherche", ge=-90, le=90),
    longitude: Optional[float] = Query(None, description="Longitude centre recherche", ge=-180, le=180),
    rayon_km: Optional[float] = Query(10, description="Rayon de recherche en km", ge=0.1, le=100),
    
    # Filtres de risque
    seuil_risque: Optional[float] = Query(0.7, description="Seuil minimum score de risque", ge=0, le=1),
    min_accidents: Optional[int] = Query(10, description="Nombre minimum d'accidents", ge=1),
    type_infrastructure: Optional[str] = Query(None, description="Type d'infrastructure"),
    
    # Pagination
    page: int = Query(1, ge=1),
    taille: int = Query(50, ge=1, le=200),
    
    # Dépendances
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Récupère les zones à risque selon les critères spécifiés"""
    
    # Validation recherche géographique
    if (latitude is not None) != (longitude is not None):
        raise HTTPException(
            status_code=422,
            detail="Latitude et longitude doivent être fournies ensemble"
        )
    
    service = HotspotService(db_session)
    
    if latitude and longitude:
        # Recherche par proximité géographique
        hotspots = await service.rechercher_par_proximite(
            latitude, longitude, rayon_km, seuil_risque, min_accidents
        )
    else:
        # Recherche par filtres administratifs
        hotspots = await service.lister_hotspots(
            etat=etat,
            comte=comte,
            seuil_risque=seuil_risque,
            min_accidents=min_accidents,
            type_infrastructure=type_infrastructure,
            page=page,
            taille=taille
        )
    
    return hotspots

@router.get(
    "/carte",
    summary="Données pour visualisation cartographique",
    description="Fournit les données optimisées pour l'affichage sur une carte interactive"
)
async def donnees_carte_hotspots(
    # Bounds de la carte
    nord: float = Query(..., description="Latitude nord", ge=-90, le=90),
    sud: float = Query(..., description="Latitude sud", ge=-90, le=90),
    est: float = Query(..., description="Longitude est", ge=-180, le=180),
    ouest: float = Query(..., description="Longitude ouest", ge=-180, le=180),
    
    # Niveau de zoom (influence la densité des points)
    niveau_zoom: int = Query(10, description="Niveau de zoom carte", ge=1, le=18),
    
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Fournit les données de hotspots pour affichage cartographique"""
    
    # Validation des bounds
    if nord <= sud or est <= ouest:
        raise HTTPException(
            status_code=422,
            detail="Coordonnées de la carte invalides"
        )
    
    service = HotspotService(db_session)
    donnees_carte = await service.obtenir_donnees_carte(
        bounds={
            "nord": nord,
            "sud": sud,
            "est": est,
            "ouest": ouest
        },
        niveau_zoom=niveau_zoom
    )
    
    return {
        "hotspots": donnees_carte["points"],
        "clusters": donnees_carte["clusters"],
        "statistiques": {
            "total_hotspots": len(donnees_carte["points"]),
            "score_risque_moyen": donnees_carte["stats"]["score_moyen"],
            "zone_plus_critique": donnees_carte["stats"]["zone_critique"]
        },
        "legende": {
            "couleurs": {
                "risque_faible": "#yellow",
                "risque_moyen": "#orange", 
                "risque_eleve": "#red",
                "risque_critique": "#darkred"
            },
            "seuils": [0.5, 0.7, 0.85, 1.0]
        }
    }

@router.get(
    "/{hotspot_id}/details",
    response_model=HotspotResponse,
    summary="Détails d'une zone à risque",
    description="Récupère les informations détaillées d'une zone à risque spécifique"
)
async def details_hotspot(
    hotspot_id: int = Path(..., description="ID du hotspot"),
    inclure_historique: bool = Query(False, description="Inclure l'historique des accidents"),
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Récupère les détails complets d'un hotspot"""
    
    service = HotspotService(db_session)
    hotspot = await service.obtenir_details_hotspot(hotspot_id, inclure_historique)
    
    if not hotspot:
        raise HTTPException(
            status_code=404,
            detail=f"Hotspot avec l'ID {hotspot_id} non trouvé"
        )
    
    return hotspot
```

### 3. Router KPIs

```python
# src/api/routers/kpis.py
from fastapi import APIRouter, Depends, Query, HTTPException, Path
from typing import Optional, List
from enum import Enum
from datetime import date, datetime, timedelta

router = APIRouter()

class MetriqueKPI(str, Enum):
    """Énumération des métriques KPI disponibles"""
    accidents_par_heure = "accidents_par_heure"
    severite_moyenne = "severite_moyenne"
    hotspots_count = "hotspots_count"
    weather_impact = "weather_impact"
    infrastructure_risk = "infrastructure_risk"
    temporal_patterns = "temporal_patterns"

class PeriodeAnalyse(str, Enum):
    """Périodes d'analyse disponibles"""
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"
    yearly = "yearly"

@router.get(
    "/{metrique}",
    summary="Récupérer un KPI spécifique",
    description="""
    Récupère les valeurs d'un indicateur clé de performance spécifique.
    
    **Métriques disponibles :**
    - `accidents_par_heure` : Nombre d'accidents par heure de la journée
    - `severite_moyenne` : Sévérité moyenne des accidents
    - `hotspots_count` : Nombre de zones à risque identifiées
    - `weather_impact` : Impact des conditions météorologiques
    - `infrastructure_risk` : Risque par type d'infrastructure
    - `temporal_patterns` : Patterns temporels des accidents
    """
)
async def obtenir_kpi(
    metrique: MetriqueKPI = Path(..., description="Type de métrique KPI"),
    
    # Filtres temporels
    periode: PeriodeAnalyse = Query(PeriodeAnalyse.daily, description="Période d'analyse"),
    date_debut: Optional[date] = Query(None, description="Date début analyse"),
    date_fin: Optional[date] = Query(None, description="Date fin analyse"),
    
    # Filtres géographiques
    granularite: Optional[str] = Query("state", description="Granularité géographique", regex="^(state|county|city)$"),
    etat: Optional[str] = Query(None, description="Filtrer par état", regex="^[A-Z]{2}$"),
    
    # Options d'affichage
    inclure_tendance: bool = Query(True, description="Inclure l'analyse de tendance"),
    inclure_comparaison: bool = Query(True, description="Inclure la comparaison période précédente"),
    
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Récupère un KPI spécifique avec ses analyses"""
    
    # Définir les dates par défaut si non fournies
    if not date_fin:
        date_fin = date.today()
    if not date_debut:
        if periode == PeriodeAnalyse.daily:
            date_debut = date_fin - timedelta(days=30)
        elif periode == PeriodeAnalyse.weekly:
            date_debut = date_fin - timedelta(weeks=12)
        elif periode == PeriodeAnalyse.monthly:
            date_debut = date_fin - timedelta(days=365)
        else:  # yearly
            date_debut = date_fin - timedelta(days=365*3)
    
    service = KPIService(db_session)
    
    try:
        kpi_data = await service.calculer_kpi(
            metrique=metrique.value,
            periode=periode.value,
            date_debut=date_debut,
            date_fin=date_fin,
            granularite=granularite,
            etat=etat
        )
        
        resultat = {
            "metrique": metrique.value,
            "periode": periode.value,
            "date_debut": date_debut,
            "date_fin": date_fin,
            "granularite": granularite,
            "valeurs": kpi_data["valeurs"],
            "statistiques": {
                "valeur_actuelle": kpi_data["valeur_actuelle"],
                "valeur_moyenne": kpi_data["valeur_moyenne"],
                "valeur_min": kpi_data["valeur_min"],
                "valeur_max": kpi_data["valeur_max"],
            }
        }
        
        # Ajouter l'analyse de tendance si demandée
        if inclure_tendance:
            resultat["tendance"] = await service.analyser_tendance(kpi_data["valeurs"])
        
        # Ajouter la comparaison avec la période précédente
        if inclure_comparaison:
            resultat["comparaison"] = await service.comparer_periode_precedente(
                metrique.value, periode.value, date_debut, date_fin
            )
        
        return resultat
        
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul du KPI: {str(e)}")

@router.get(
    "/",
    summary="Tableau de bord des KPIs",
    description="Récupère un tableau de bord avec tous les KPIs principaux"
)
async def tableau_bord_kpis(
    periode: PeriodeAnalyse = Query(PeriodeAnalyse.daily),
    etat: Optional[str] = Query(None, regex="^[A-Z]{2}$"),
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Récupère un tableau de bord complet des KPIs"""
    
    service = KPIService(db_session)
    
    # Calculer tous les KPIs principaux
    kpis = {}
    for metrique in MetriqueKPI:
        try:
            kpi_data = await service.calculer_kpi(
                metrique=metrique.value,
                periode=periode.value,
                etat=etat
            )
            kpis[metrique.value] = {
                "valeur_actuelle": kpi_data["valeur_actuelle"],
                "evolution": kpi_data.get("evolution", 0),
                "statut": kpi_data.get("statut", "normal")
            }
        except Exception as e:
            kpis[metrique.value] = {
                "erreur": str(e),
                "statut": "erreur"
            }
    
    return {
        "timestamp": datetime.now(),
        "periode": periode.value,
        "etat": etat,
        "kpis": kpis,
        "resume": {
            "total_accidents": kpis.get("accidents_par_heure", {}).get("valeur_actuelle", 0),
            "severite_globale": kpis.get("severite_moyenne", {}).get("valeur_actuelle", 0),
            "zones_critiques": kpis.get("hotspots_count", {}).get("valeur_actuelle", 0),
        }
    }
```

### 4. Router Prédictions ML

```python
# src/api/routers/predictions.py
from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator

from ..models.request_models import PredictionRequest
from ..models.response_models import PredictionResponse
from ..services.prediction_service import PredictionService

router = APIRouter()

@router.post(
    "/predict",
    response_model=PredictionResponse,
    summary="Prédire la sévérité d'un accident",
    description="""
    Prédit la sévérité d'un accident potentiel basé sur les conditions fournies.
    
    **Données requises :**
    - Localisation géographique (latitude, longitude)
    - Conditions météorologiques
    - Informations sur l'infrastructure
    - Contexte temporel
    
    **Modèle utilisé :**
    Le modèle de machine learning entraîné sur l'historique des accidents US
    utilise un ensemble de features pour prédire la sévérité (1-4).
    """
)
async def predire_severite(
    donnees_prediction: PredictionRequest = Body(
        ...,
        example={
            "localisation": {
                "latitude": 39.8651,
                "longitude": -84.0587
            },
            "conditions_meteo": {
                "temperature": 36.9,
                "humidite": 91.0,
                "visibilite": 10.0,
                "condition": "Light Rain",
                "vitesse_vent": 3.5
            },
            "infrastructure": {
                "type_route": "I-70 E",
                "presence_feux": True,
                "intersection": False,
                "rond_point": False,
                "zone_travaux": False
            },
            "contexte_temporel": {
                "heure": 5,
                "jour_semaine": 1,
                "mois": 2,
                "saison": "winter"
            }
        }
    ),
    modele_version: Optional[str] = Query(None, description="Version spécifique du modèle"),
    inclure_explication: bool = Query(True, description="Inclure l'explication de la prédiction"),
    db_session = Depends(get_db_session),
    utilisateur_actuel = Depends(get_current_user)
):
    """Prédit la sévérité d'un accident basé sur les conditions"""
    
    service = PredictionService(db_session)
    
    try:
        # Validation des données d'entrée
        await service.valider_donnees_entree(donnees_prediction)
        
        # Effectuer la prédiction
        prediction = await service.predire_severite(
            donnees_prediction,
            modele_version=modele_version,
            inclure_explication=inclure_explication
        )
        
        # Sauvegarder la prédiction pour suivi
        await