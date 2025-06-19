#!/bin/bash
echo "========================================"
echo "   DÉMARRAGE DASHBOARD STREAMLIT"
echo "========================================"
echo

echo "Vérification des dépendances..."
pip install -r requirements.txt

echo
echo "Démarrage du dashboard Streamlit..."
echo "Interface disponible sur: http://localhost:8501"
echo

streamlit run app.py