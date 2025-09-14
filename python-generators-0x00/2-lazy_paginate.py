#!/usr/bin/python3
import mysql.connector

def paginate_users(page_size, offset):
    """
    Fetches a single page of rows from user_data using LIMIT/OFFSET.
    Returns a list of dicts.
    """
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='THELordISMyCatch1101?',
        database='ALX_prodev'
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT user_id, name, email, age "
        "FROM user_data "
        "LIMIT %s OFFSET %s",
        (page_size, offset)
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def lazy_pagination(page_size):
    """
    Generator that lazily yields pages of users (each a list of dicts).
    Uses exactly one loop to fetch pages until none remain.
    """
    offset = 0
    while True:                    # ← Single loop
        page = paginate_users(page_size, offset)
        if not page:
            break
        yield page
        offset += page_size
