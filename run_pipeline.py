"""
Punto de entrada del pipeline ETL.

Uso básico:
    python run_pipeline.py

Con variables de entorno para MySQL:
    DB_HOST=localhost DB_USER=etl_user DB_PASSWORD=secreto DB_NAME=etl_db python run_pipeline.py

Con sal personalizada para el hash:
    ETL_SALT="mi_sal_secreta" python run_pipeline.py
"""

from etl_pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline()
