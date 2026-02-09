import requests
import pytest
import psycopg2
from unittest.mock import patch

def test_youtube_api_response(airflow_variable):
    # 1. Get the variables
    api_key = airflow_variable("API_KEY")
    channel_handle = airflow_variable("CHANNEL_HANDLE")
    
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    
    # 2. MOCK the internet.
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"items": [{"id": "123", "contentDetails": {}}]}
        
        try:
            response = requests.get(url)
            assert response.status_code == 200
            
            # Verify URL
            expected_url = "https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle=MRCHEESE&key=MOCK_KEY1234"
            mock_get.assert_called_with(expected_url)
            
        except requests.RequestException as e:
            pytest.fail(f"API request failed: {e}")

def test_real_postgres_connection(real_postgres_connection):
    # FIX 1: Guard clause. If setup failed, fail the test immediately with a clear message.
    if real_postgres_connection is None:
        pytest.fail("Database connection is None. Check your environment variables (ELT_DATABASE_USER, etc).")

    cursor = None
    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        
        # FIX 2: Correctly fetch and assert the result
        row = cursor.fetchone()
        assert row is not None, "Query returned no results"
        assert row[0] == 1, f"Expected 1, got {row[0]}"

    except psycopg2.Error as e:
        pytest.fail(f"Postgres query failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()