import sqlite3
import functools

def log_queries(func):
    """
    Decorator that logs the SQL query before calling the decorated function.
    Expects the wrapped function to accept the SQL string as either:
      - a keyword argument named 'query'
      - or as its first positional argument
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Determine the SQL query argument
        sql = kwargs.get('query')
        if sql is None and args:
            sql = args[0]
        # Log the query
        print(f"Executing query: {sql}")
        # Call the original function
        return func(*args, **kwargs)
    return wrapper

@log_queries
def fetch_all_users(query):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

### fetch users while logging the query
users = fetch_all_users(query="SELECT * FROM users")
