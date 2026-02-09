import os
import pytest
import psycopg2
from unittest import mock
# Try/Except to prevent crashing if Airflow isn't installed locally
try:
    from airflow.models import Variable, Connection, DagBag
except ImportError:
    pass

@pytest.fixture
def mock_airflow_env():
    """
    Sets up the environment variables so Variable.get() works.
    """
    env_vars = {
        "AIRFLOW_VAR_API_KEY": "MOCK_KEY1234",
        "AIRFLOW_VAR_CHANNEL_HANDLE": "MRCHEESE",
        # Add connection URIs here if needed
    }
    with mock.patch.dict("os.environ", env_vars):
        yield

@pytest.fixture
def airflow_variable(mock_airflow_env):
    """
    Returns a helper function to get variables.
    Requires 'mock_airflow_env' to run first so the vars exist.
    """
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    
    return get_airflow_variable

@pytest.fixture
def real_postgres_connection():
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        yield conn
    
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        yield None
    
    finally:
        if conn:
            conn.close()