@echo off
echo ========================================
echo    DEMARRAGE DASHBOARD STREAMLIT
echo ========================================
echo.

echo Verification des dependances...
pip install -r requirements.txt

echo.
echo Demarrage du dashboard Streamlit...
echo Interface disponible sur: http://localhost:8501
echo.

streamlit run app.py