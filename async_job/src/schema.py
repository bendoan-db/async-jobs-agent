"""
Database schema definitions for async job tasks.

This module contains SQL schema definitions for tables used by
the async job tasks, ensuring consistency across all tasks.
"""

# Schema for task execution logs
TASK_LOGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_name VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    status VARCHAR(50) NOT NULL
)
"""


def ensure_task_logs_table_exists(conn) -> None:
    """
    Create the task_logs table if it doesn't exist.

    Args:
        conn: psycopg2 connection object
    """
    with conn.cursor() as cur:
        cur.execute(TASK_LOGS_SCHEMA)
    conn.commit()
