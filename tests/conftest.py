import os
import pytest
import psycopg2
from unittest import mock
from airflow.models import DagBag

# --- 1. Fixtures for Airflow Variables (EXISTING) ---
@pytest.fixture
def mock_airflow_env():
    """Sets up the environment variables so Variable.get() works."""
    env_vars = {
        "AIRFLOW_VAR_API_KEY": "MOCK_KEY1234",
        "AIRFLOW_VAR_CHANNEL_HANDLE": "MRCHEESE",
    }
    with mock.patch.dict("os.environ", env_vars):
        yield

@pytest.fixture
def airflow_variable(mock_airflow_env):
    """Returns a helper function to get variables."""
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    return get_airflow_variable

# --- 2. Fixtures for Database Integration (EXISTING) ---
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

# --- 3. MISSING FIXTURES (ADD THESE TO FIX ERRORS) ---

@pytest.fixture
def api_key():
    """Provides a dummy API key for unit tests."""
    return "MOCK_API_KEY_12345"

@pytest.fixture
def channel_handle():
    """Provides a dummy channel handle for unit tests."""
    return "@MockChannel"

@pytest.fixture
def mock_postgres_conn_vars(monkeypatch):
    """
    Sets fake environment variables so unit tests don't try 
    to connect to the real production database.
    """
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "test_db")

@pytest.fixture
def dagbag():
    """
    Loads Airflow DAGs for integrity testing.
    It looks for the 'dags' folder in the standard Airflow path.
    """
    # Default to /opt/airflow/dags if AIRFLOW_HOME isn't set
    dag_folder = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags')
    
    # Initialize DagBag (this reads your DAG files)
    return DagBag(dag_folder=dag_folder, include_examples=False)