def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"
    assert conn.login == "mock_user"
    assert conn.password == "mock_password"
    
def test_dags_integrity (dagbag):
    #1. Check for import errors
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"
    print("====================")
    print(dagbag.import_errors)
    #2. Check for dag integrity
    expected_dag_ids = {"produce_json", "update_db", "data_quality"}
    loaded_dag_ids = list(dagbag.dag_ids)
    print("====================")
    print(dagbag.dag_ids)
    
    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} not found in dagbag"
    
    #3. Check for dag dependencies
    assert dagbag.size() == 3
    print("====================")
    print(dagbag.size())

    #4. Check for dag dependencies
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }

    print("====================")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id, len(dag.tasks))
        