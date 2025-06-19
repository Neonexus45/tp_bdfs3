import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import mysql.connector
from datetime import datetime, timedelta
import json
import numpy as np
from typing import Dict, List, Optional
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.common.config.config_manager import ConfigManager

class StreamlitDashboard:
    def __init__(self):
        self.config = ConfigManager()
        self.db_config = {
            'host': self.config.get('DB_HOST', 'localhost'),
            'port': int(self.config.get('DB_PORT', 3306)),
            'user': self.config.get('DB_USER', 'tatane'),
            'password': self.config.get('DB_PASSWORD', 'tatane'),
            'database': self.config.get('DB_NAME', 'accidents_db')
        }
    
    def get_connection(self):
        try:
            return mysql.connector.connect(**self.db_config)
        except Exception as e:
            st.error(f"Erreur de connexion à MySQL: {e}")
            return None
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        conn = self.get_connection()
        if conn is None:
            return None
        
        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            st.error(f"Erreur d'exécution de la requête: {e}")
            return None
        finally:
            conn.close()
    
    def load_accidents_summary(self) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            state, city, severity, accident_date, accident_hour,
            weather_category, temperature_category, infrastructure_count,
            safety_score, distance_miles
        FROM accidents_summary 
        ORDER BY accident_date DESC 
        LIMIT 10000
        """
        return self.execute_query(query)
    
    def load_kpis_security(self) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            state, city, accident_rate_per_100k, danger_index,
            severity_distribution, hotspot_rank, last_updated
        FROM kpis_security 
        ORDER BY danger_index DESC
        """
        return self.execute_query(query)
    
    def load_kpis_temporal(self) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            period_type, period_value, state, accident_count,
            severity_avg, trend_direction, seasonal_factor
        FROM kpis_temporal 
        ORDER BY period_value DESC
        """
        return self.execute_query(query)
    
    def load_hotspots(self) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            state, city, latitude, longitude, accident_count,
            severity_avg, danger_score, radius_miles
        FROM hotspots 
        ORDER BY danger_score DESC 
        LIMIT 100
        """
        return self.execute_query(query)
    
    def load_ml_performance(self) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            model_name, model_version, accuracy, precision_score,
            recall_score, f1_score, feature_importance, training_date
        FROM ml_model_performance 
        ORDER BY training_date DESC
        """
        return self.execute_query(query)

def main():
    st.set_page_config(
        page_title="Dashboard Lakehouse US-Accidents",
        page_icon="🚗",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🚗 Dashboard Lakehouse US-Accidents")
    st.markdown("**Analyse des accidents de la route aux États-Unis - Architecture Medallion**")
    
    dashboard = StreamlitDashboard()
    
    # Sidebar pour navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choisir une page",
        ["🏠 Accueil", "📊 KPIs Sécurité", "⏰ Analyse Temporelle", 
         "🗺️ Hotspots Géographiques", "🤖 Performance ML", "📈 Données Brutes"]
    )
    
    if page == "🏠 Accueil":
        show_home_page(dashboard)
    elif page == "📊 KPIs Sécurité":
        show_security_kpis(dashboard)
    elif page == "⏰ Analyse Temporelle":
        show_temporal_analysis(dashboard)
    elif page == "🗺️ Hotspots Géographiques":
        show_geographic_hotspots(dashboard)
    elif page == "🤖 Performance ML":
        show_ml_performance(dashboard)
    elif page == "📈 Données Brutes":
        show_raw_data(dashboard)

def show_home_page(dashboard):
    st.header("🏠 Vue d'ensemble du Lakehouse")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Métriques générales
    accidents_df = dashboard.load_accidents_summary()
    if accidents_df is not None:
        with col1:
            st.metric("Total Accidents", f"{len(accidents_df):,}")
        with col2:
            avg_severity = accidents_df['severity'].mean()
            st.metric("Sévérité Moyenne", f"{avg_severity:.2f}")
        with col3:
            states_count = accidents_df['state'].nunique()
            st.metric("États Couverts", states_count)
        with col4:
            latest_date = accidents_df['accident_date'].max()
            st.metric("Dernière Mise à Jour", latest_date.strftime("%Y-%m-%d"))
    
    st.markdown("---")
    
    # Architecture Medallion
    st.subheader("🏗️ Architecture Medallion Implémentée")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        **🥉 Bronze Layer**
        - Source: CSV US-Accidents
        - Stockage: HDFS Parquet
        - Partitioning: Date/État
        - Application: FEEDER
        """)
    
    with col2:
        st.markdown("""
        **🥈 Silver Layer**
        - Nettoyage des données
        - Feature Engineering
        - Stockage: Hive ORC
        - Application: PREPROCESSOR
        """)
    
    with col3:
        st.markdown("""
        **🥇 Gold Layer**
        - Agrégations Business
        - KPIs Calculés
        - Stockage: MySQL
        - Application: DATAMART
        """)
    
    with col4:
        st.markdown("""
        **🤖 ML Layer**
        - Modèles Prédictifs
        - Évaluation Performance
        - Stockage: MLflow
        - Application: MLTRAINING
        """)

def show_security_kpis(dashboard):
    st.header("📊 KPIs de Sécurité Routière")
    
    kpis_df = dashboard.load_kpis_security()
    if kpis_df is None or len(kpis_df) == 0:
        st.warning("Aucune donnée KPI sécurité disponible. Exécutez d'abord le pipeline DATAMART.")
        return
    
    # Top 10 états les plus dangereux
    st.subheader("🚨 Top 10 États les Plus Dangereux")
    top_dangerous = kpis_df.nlargest(10, 'danger_index')
    
    fig = px.bar(
        top_dangerous, 
        x='state', 
        y='danger_index',
        title="Index de Dangerosité par État",
        labels={'danger_index': 'Index de Dangerosité', 'state': 'État'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Taux d'accidents par 100k habitants
    st.subheader("📈 Taux d'Accidents par 100k Habitants")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.scatter(
            kpis_df,
            x='accident_rate_per_100k',
            y='danger_index',
            hover_data=['state', 'city'],
            title="Corrélation Taux d'Accidents vs Index de Dangerosité"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Distribution des hotspot ranks
        fig = px.histogram(
            kpis_df,
            x='hotspot_rank',
            title="Distribution des Rankings Hotspots",
            nbins=20
        )
        st.plotly_chart(fig, use_container_width=True)

def show_temporal_analysis(dashboard):
    st.header("⏰ Analyse Temporelle des Accidents")
    
    temporal_df = dashboard.load_kpis_temporal()
    if temporal_df is None or len(temporal_df) == 0:
        st.warning("Aucune donnée temporelle disponible. Exécutez d'abord le pipeline DATAMART.")
        return
    
    # Filtres
    period_types = temporal_df['period_type'].unique()
    selected_period = st.selectbox("Type de période", period_types)
    
    filtered_df = temporal_df[temporal_df['period_type'] == selected_period]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Évolution du nombre d'accidents
        fig = px.line(
            filtered_df,
            x='period_value',
            y='accident_count',
            color='state',
            title=f"Évolution des Accidents - {selected_period}"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Sévérité moyenne par période
        fig = px.bar(
            filtered_df.groupby('period_value')['severity_avg'].mean().reset_index(),
            x='period_value',
            y='severity_avg',
            title=f"Sévérité Moyenne - {selected_period}"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Facteur saisonnier
    st.subheader("🌡️ Analyse Saisonnière")
    seasonal_data = filtered_df.groupby('period_value').agg({
        'seasonal_factor': 'mean',
        'accident_count': 'sum'
    }).reset_index()
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Scatter(x=seasonal_data['period_value'], y=seasonal_data['seasonal_factor'], name="Facteur Saisonnier"),
        secondary_y=False,
    )
    
    fig.add_trace(
        go.Bar(x=seasonal_data['period_value'], y=seasonal_data['accident_count'], name="Nombre d'Accidents"),
        secondary_y=True,
    )
    
    fig.update_yaxes(title_text="Facteur Saisonnier", secondary_y=False)
    fig.update_yaxes(title_text="Nombre d'Accidents", secondary_y=True)
    fig.update_layout(title_text="Corrélation Saisonnalité vs Accidents")
    
    st.plotly_chart(fig, use_container_width=True)

def show_geographic_hotspots(dashboard):
    st.header("🗺️ Hotspots Géographiques")
    
    hotspots_df = dashboard.load_hotspots()
    if hotspots_df is None or len(hotspots_df) == 0:
        st.warning("Aucune donnée hotspots disponible. Exécutez d'abord le pipeline DATAMART.")
        return
    
    # Carte des hotspots
    st.subheader("🌍 Carte des Zones Dangereuses")
    
    fig = px.scatter_mapbox(
        hotspots_df,
        lat="latitude",
        lon="longitude",
        size="danger_score",
        color="severity_avg",
        hover_name="city",
        hover_data=["state", "accident_count"],
        color_continuous_scale="Reds",
        size_max=15,
        zoom=3,
        mapbox_style="open-street-map",
        title="Hotspots d'Accidents par Zone Géographique"
    )
    
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # Top hotspots par état
    st.subheader("🏆 Top Hotspots par État")
    
    col1, col2 = st.columns(2)
    
    with col1:
        top_hotspots = hotspots_df.nlargest(15, 'danger_score')
        fig = px.bar(
            top_hotspots,
            x='danger_score',
            y='city',
            orientation='h',
            color='state',
            title="Top 15 Villes les Plus Dangereuses"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Analyse par état
        state_analysis = hotspots_df.groupby('state').agg({
            'danger_score': 'mean',
            'accident_count': 'sum',
            'severity_avg': 'mean'
        }).reset_index()
        
        fig = px.scatter(
            state_analysis,
            x='accident_count',
            y='danger_score',
            size='severity_avg',
            hover_data=['state'],
            title="Analyse des États: Accidents vs Dangerosité"
        )
        st.plotly_chart(fig, use_container_width=True)

def show_ml_performance(dashboard):
    st.header("🤖 Performance des Modèles ML")
    
    ml_df = dashboard.load_ml_performance()
    if ml_df is None or len(ml_df) == 0:
        st.warning("Aucune donnée ML disponible. Exécutez d'abord le pipeline MLTRAINING.")
        return
    
    # Métriques des modèles
    st.subheader("📊 Métriques de Performance")
    
    col1, col2, col3, col4 = st.columns(4)
    
    latest_model = ml_df.iloc[0]
    with col1:
        st.metric("Accuracy", f"{latest_model['accuracy']:.3f}")
    with col2:
        st.metric("Precision", f"{latest_model['precision_score']:.3f}")
    with col3:
        st.metric("Recall", f"{latest_model['recall_score']:.3f}")
    with col4:
        st.metric("F1-Score", f"{latest_model['f1_score']:.3f}")
    
    # Comparaison des modèles
    st.subheader("🔄 Comparaison des Modèles")
    
    metrics_df = ml_df[['model_name', 'accuracy', 'precision_score', 'recall_score', 'f1_score']].melt(
        id_vars=['model_name'], 
        var_name='metric', 
        value_name='score'
    )
    
    fig = px.bar(
        metrics_df,
        x='model_name',
        y='score',
        color='metric',
        barmode='group',
        title="Comparaison des Métriques par Modèle"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Feature importance
    if 'feature_importance' in ml_df.columns and pd.notna(latest_model['feature_importance']):
        st.subheader("🎯 Importance des Features")
        try:
            feature_importance = json.loads(latest_model['feature_importance'])
            if isinstance(feature_importance, dict):
                importance_df = pd.DataFrame(
                    list(feature_importance.items()),
                    columns=['feature', 'importance']
                ).sort_values('importance', ascending=True).tail(15)
                
                fig = px.bar(
                    importance_df,
                    x='importance',
                    y='feature',
                    orientation='h',
                    title="Top 15 Features les Plus Importantes"
                )
                st.plotly_chart(fig, use_container_width=True)
        except:
            st.info("Données d'importance des features non disponibles")

def show_raw_data(dashboard):
    st.header("📈 Exploration des Données")
    
    accidents_df = dashboard.load_accidents_summary()
    if accidents_df is None:
        st.warning("Aucune donnée disponible. Exécutez d'abord le pipeline complet.")
        return
    
    st.subheader("🔍 Aperçu des Données")
    st.write(f"**Nombre total d'enregistrements:** {len(accidents_df):,}")
    st.write(f"**Période couverte:** {accidents_df['accident_date'].min()} à {accidents_df['accident_date'].max()}")
    
    # Filtres
    col1, col2, col3 = st.columns(3)
    
    with col1:
        states = ['Tous'] + sorted(accidents_df['state'].unique().tolist())
        selected_state = st.selectbox("État", states)
    
    with col2:
        severities = ['Tous'] + sorted(accidents_df['severity'].unique().tolist())
        selected_severity = st.selectbox("Sévérité", severities)
    
    with col3:
        weather_cats = ['Tous'] + sorted(accidents_df['weather_category'].dropna().unique().tolist())
        selected_weather = st.selectbox("Météo", weather_cats)
    
    # Application des filtres
    filtered_df = accidents_df.copy()
    if selected_state != 'Tous':
        filtered_df = filtered_df[filtered_df['state'] == selected_state]
    if selected_severity != 'Tous':
        filtered_df = filtered_df[filtered_df['severity'] == selected_severity]
    if selected_weather != 'Tous':
        filtered_df = filtered_df[filtered_df['weather_category'] == selected_weather]
    
    st.write(f"**Données filtrées:** {len(filtered_df):,} enregistrements")
    
    # Affichage des données
    st.subheader("📋 Données Détaillées")
    st.dataframe(filtered_df.head(1000), use_container_width=True)
    
    # Statistiques descriptives
    st.subheader("📊 Statistiques Descriptives")
    st.write(filtered_df.describe())

if __name__ == "__main__":
    main()