import os
import logging
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any, List, Union
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Database connection pool
connection_pool: Optional[pool.ThreadedConnectionPool] = None

def get_db_config() -> Dict[str, Any]:
    """
    Get database configuration from environment variables.
    Supports both DATABASE_URL and individual connection parameters.
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return {"database_url": database_url}
    
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }

def init_db_pool(min_conn: int = 1, max_conn: int = 5):
    """
    Initialize database connection pool.
    """
    global connection_pool
    
    if connection_pool:
        return
    
    try:
        config = get_db_config()
        
        if "database_url" in config:
            connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                config["database_url"]
            )
        else:
            connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["user"],
                password=config["password"],
            )
        
        logger.info("Database connection pool initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize database connection pool: {str(e)}")
        raise

def close_db_pool():
    """
    Close all database connections in the pool.
    """
    global connection_pool
    
    if connection_pool:
        connection_pool.closeall()
        connection_pool = None
        logger.info("Database connection pool closed")

@contextmanager
def get_db_connection():
    """
    Context manager for getting a database connection from the pool.
    Returns a connection with RealDictCursor for dict-like row access.
    """
    global connection_pool
    
    if not connection_pool:
        init_db_pool()
    
    conn = None
    try:
        conn = connection_pool.getconn()
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            connection_pool.putconn(conn)

def execute_query(query: str, params: Optional[tuple] = None, fetch_one: bool = False) -> Union[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Execute a SELECT query and return results as list of dictionaries.
    
    Args:
        query: SQL query string
        params: Query parameters tuple
        fetch_one: If True, return only first result
        
    Returns:
        List of dictionaries, or single dict if fetch_one=True, or None if fetch_one=True and no results
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            
            if fetch_one:
                result = cur.fetchone()
                return dict(result) if result else None
            else:
                results = cur.fetchall()
                return [dict(row) for row in results]

def execute_write(query: str, params: Optional[tuple] = None) -> int:
    """
    Execute an INSERT/UPDATE/DELETE query and return affected rows.
    
    Args:
        query: SQL query string
        params: Query parameters tuple
        
    Returns:
        Number of affected rows
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            return cur.rowcount

def test_connection() -> bool:
    """
    Test database connection.
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False

