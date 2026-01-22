"""
Task 2: Print message and log to Lakebase

This task prints "task 2 done" and logs the message to Lakebase for persistence.
"""

import argparse
import uuid
from datetime import datetime, timezone

import psycopg2
from databricks.sdk import WorkspaceClient


def get_lakebase_connection(instance_name: str):
    """
    Create a connection to Lakebase instance.

    Args:
        instance_name: Name of the Lakebase instance

    Returns:
        psycopg2 connection object
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


def ensure_log_table_exists(conn) -> None:
    """Create the task_logs table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS task_logs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                task_name VARCHAR(255) NOT NULL,
                message TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                status VARCHAR(50) NOT NULL
            )
        """)
    conn.commit()


def log_to_lakebase(instance_name: str, message: str, task_name: str) -> None:
    """
    Log a message to Lakebase.

    Args:
        instance_name: Name of the Lakebase instance
        message: Message to log
        task_name: Name of the task for identification
    """
    conn = get_lakebase_connection(instance_name)

    try:
        # Ensure table exists
        ensure_log_table_exists(conn)

        # Insert log entry
        timestamp = datetime.now(timezone.utc)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO task_logs (task_name, message, timestamp, status)
                VALUES (%s, %s, %s, %s)
                """,
                (task_name, message, timestamp, "completed")
            )
        conn.commit()

        print(f"Logged to Lakebase ({instance_name}): task_name={task_name}, message={message}")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Task 2")
    parser.add_argument(
        "--lakebase-instance",
        required=True,
        help="Lakebase instance name for logging",
    )
    args = parser.parse_args()

    # Print the message
    message = "task 2 done"
    print(message)

    # Log to Lakebase
    log_to_lakebase(
        instance_name=args.lakebase_instance,
        message=message,
        task_name="task_2",
    )

    print("Task 2 completed successfully!")


if __name__ == "__main__":
    main()
