# log_queries_decorator.py

import functools
import logging
import inspect
import sqlite3

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more verbose output
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def log_queries(level=logging.INFO):
    """
    Decorator factory that logs SQL queries before executing the wrapped function.

    Supports functions that accept a SQL query string either as the first positional
    argument or as a keyword argument named 'query'.

    Parameters:
        level (int): Logging level for the query (default: INFO).
    Usage:
        @log_queries()
        def db_func(query):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            query = None
            # Inspect the function's signature for robust argument resolution
            try:
                sig = inspect.signature(func)
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                params = bound.arguments
                if 'query' in params:
                    query = params['query']
            except Exception:
                # Fallback for callables not supporting signature introspection
                query = None

            # Fallback: try keyword first, then positional
            if query is None:
                query = kwargs.get('query')
            if query is None and args:
                query = args[0]  # Assumes first positional = query string

            # Log with appropriate level and context
            if isinstance(query, str):
                logger.log(level, "Executing SQL Query: %s", query)
            else:
                logger.warning("No SQL query found in arguments for %s.", func.__qualname__)

            # Let the original function execute and return result
            return func(*args, **kwargs)
        return wrapper
    return decorator

@log_queries()
def fetch_all_users(query):
    """
    Executes the provided SQL query using sqlite3 and returns all results.

    Args:
        query (str): The SQL query string to execute.

    Returns:
        list: Query results as a list of tuples.
    """
    # In-memory demonstration DB for this example
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Set up schema and seed test data for demonstration only
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.executemany(
        "INSERT INTO users (name) VALUES (?)",
        [("Alice",), ("Bob",), ("Charlie",)]
    )
    conn.commit()

    # Execute the requested query and fetch all results
    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    return results

# Example usage and demonstration
if __name__ == "__main__":
    user_query = "SELECT * FROM users"
    users = fetch_all_users(user_query)
    print("Query Results:")
    for user in users:
        print(user)
