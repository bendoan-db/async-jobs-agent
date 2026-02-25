"""
Lakebase utilities for async job tasks.

This module provides shared functions for connecting to and
logging data to Databricks Lakebase.
"""

import uuid
from datetime import datetime, timezone

import psycopg2
from databricks.sdk import WorkspaceClient

try:
    from .schema import ensure_task_logs_table_exists
except ImportError:
    from schema import ensure_task_logs_table_exists


def get_lakebase_connection(instance_name: str):
    """
    Create a connection to a Lakebase instance.

    Args:
        instance_name: Name of the Lakebase instance

    Returns:
        psycopg2 connection object

    Example:
        conn = get_lakebase_connection("my-lakebase-instance")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM my_table")
        finally:
            conn.close()
    """
    w = WorkspaceClient()

    # Get instance details
    instance = w.database.get_database_instance(name=instance_name)

    # Generate credentials
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name]
    )

    # Get current user for connection
    current_user = w.current_user.me()
    username = current_user.user_name

    # Connect to Lakebase
    conn = psycopg2.connect(
        host=instance.read_write_dns,
        dbname="databricks_postgres",
        user=username,
        password=cred.token,
        sslmode="require"
    )

    return conn


def log_to_lakebase(
    instance_name: str,
    task_name: str,
    message: str,
    status: str = "completed"
) -> None:
    """
    Log a message to Lakebase task_logs table.

    This function creates the task_logs table if it doesn't exist
    and inserts a log entry with the provided information.

    Args:
        instance_name: Name of the Lakebase instance
        task_name: Name of the task for identification
        message: Message to log
        status: Status of the task (default: "completed")

    Example:
        log_to_lakebase(
            instance_name="my-lakebase",
            task_name="task_1",
            message="Processing complete",
            status="completed"
        )
    """
    conn = get_lakebase_connection(instance_name)

    try:
        # Ensure table exists
        ensure_task_logs_table_exists(conn)

        # Insert log entry
        timestamp = datetime.now(timezone.utc)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO task_logs (task_name, message, timestamp, status)
                VALUES (%s, %s, %s, %s)
                """,
                (task_name, message, timestamp, status)
            )
        conn.commit()

        print(f"Logged to Lakebase ({instance_name}): task_name={task_name}, message={message}")

    finally:
        conn.close()
