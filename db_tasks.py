from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def check_postgres_connection(connection_id):
    """
    Task 1 Logic: Connect to Postgres and verify connectivity.
    """
    logging.info(f"Attempting to connect to {connection_id}...")
    
    hook = PostgresHook(postgres_conn_id=connection_id)
    
    # Run a simple query to ensure connection is alive
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    
    logging.info(f"Connection successful. Test Result: {result}")
    return "Connection Validated"