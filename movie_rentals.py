#!/usr/bin/env python3
"""
movie_rentals.py
SQLite + Python mini-project.

- Creates a small SQLite database with `movies` and `rentals` tables.
- Seeds sample data (only if tables are empty).
- Runs two example analyses:
  1) Top N most rented movies (by rental count)
  2) Average rental duration in days (exclude rentals not yet returned)

Run:
    python movie_rentals.py

This script is heavily commented to teach you why each line is used.
"""

# --- Imports ---
import sqlite3                # built-in SQLite driver (no external DB required)
import os                     # to check if DB file exists
from datetime import datetime # parse and format dates if needed
from typing import List       # type hinting (improves readability)

# DB filename constant so we can change easily later
DB_FILE = "movie_rentals.db"

# -------------------------
# Helper: connect to DB
# -------------------------
def get_connection(db_path: str = DB_FILE) -> sqlite3.Connection:
    """
    Return a sqlite3.Connection connected to db_path.
    We set row_factory to sqlite3.Row so query results behave like dicts (nameable columns).
    """
    conn = sqlite3.connect(db_path)            # connect/create the sqlite DB file
    # row_factory makes cursor.fetchall() rows accessible by column name: row["title"]
    conn.row_factory = sqlite3.Row
    return conn

# -------------------------
# Schema: create tables
# -------------------------
def create_tables(conn: sqlite3.Connection):
    """
    Create `movies` and `rentals` tables if they don't exist.
    We use IF NOT EXISTS so this function is idempotent (safe to call multiple times).
    """
    cur = conn.cursor()                         # get a cursor to run SQL
    # movies table stores basic metadata about movies
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        genre TEXT,
        release_year INTEGER
    )
    """)
    # rentals table stores each rental event for a movie
    # rental_date and return_date are stored as TEXT in ISO format (YYYY-MM-DD)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS rentals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id INTEGER NOT NULL,
        customer_name TEXT,
        rental_date TEXT NOT NULL,
        return_date TEXT,
        FOREIGN KEY (movie_id) REFERENCES movies(id)
    )
    """)
    conn.commit()                               # persist the schema changes

# -------------------------
# Seed: sample data
# -------------------------
def seed_sample_data(conn: sqlite3.Connection):
    """
    Insert sample movies and rentals only if movies table is empty.
    This prevents re-seeding duplicate rows on subsequent runs.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM movies")
    row = cur.fetchone()
    if row and row["cnt"] > 0:
        # If we already have movies, do not reseed
        print("Database already has data — skipping seeding.")
        return

    # Sample movie list (title, genre, year)
    movies = [
        ("The Shawshank Redemption", "Drama", 1994),
        ("The Godfather", "Crime", 1972),
        ("The Dark Knight", "Action", 2008),
        ("Pulp Fiction", "Crime", 1994),
        ("Forrest Gump", "Drama", 1994),
        ("Inception", "Sci-Fi", 2010),
        ("The Matrix", "Sci-Fi", 1999),
        ("Avengers: Endgame", "Action", 2019),
        ("Interstellar", "Sci-Fi", 2014),
        ("The Lion King", "Animation", 1994),
    ]
    # Insert movies using parameterized query to avoid SQL injection and for performance
    cur.executemany("INSERT INTO movies (title, genre, release_year) VALUES (?, ?, ?)", movies)

    # Sample rental events: (movie_id, customer_name, rental_date, return_date)
    # We choose some return_dates as None to simulate rentals still out.
    rentals = [
        (1, "Alice", "2025-09-01", "2025-09-05"),
        (1, "Bob",   "2025-09-10", "2025-09-13"),
        (2, "Carol", "2025-09-01", "2025-09-03"),
        (2, "Dave",  "2025-09-15", None),           # not returned yet
        (3, "Eve",   "2025-08-30", "2025-09-02"),
        (3, "Frank", "2025-09-02", "2025-09-07"),
        (3, "Grace", "2025-09-10", "2025-09-11"),
        (4, "Heidi", "2025-09-03", "2025-09-04"),
        (5, "Ivan",  "2025-09-04", "2025-09-07"),
        (6, "Judy",  "2025-09-05", None),           # not returned yet
        (7, "Ken",   "2025-09-06", "2025-09-08"),
        (7, "Liam",  "2025-09-07", "2025-09-10"),
        (1, "Mia",   "2025-09-11", "2025-09-12"),
        (8, "Nina",  "2025-09-01", "2025-09-09"),
        (9, "Oscar", "2025-09-02", "2025-09-06"),
        (10, "Pam",  "2025-09-08", "2025-09-09"),
        (3, "Quinn", "2025-09-12", "2025-09-15"),
        (2, "Ray",   "2025-09-20", "2025-09-22"),
        (1, "Sam",   "2025-09-21", "2025-09-25"),
        (1, "Tina",  "2025-09-25", None),
    ]
    # We insert rentals with a parameterized query. For None return_date, pass None so DB stores NULL.
    cur.executemany("INSERT INTO rentals (movie_id, customer_name, rental_date, return_date) VALUES (?, ?, ?, ?)", rentals)
    conn.commit()
    print("Sample data seeded into database.")

# -------------------------
# Query: top N most rented movies
# -------------------------
def get_top_rented_movies(conn: sqlite3.Connection, limit: int = 5) -> List[sqlite3.Row]:
    """
    Return top `limit` movies by rental count.
    Query logic:
      - JOIN movies and rentals on movie_id
      - GROUP BY movies.id
      - COUNT(rentals.id) as rental_count
      - ORDER BY rental_count DESC
      - LIMIT <N>
    """
    cur = conn.cursor()
    # We join and group — using parameter substitution for LIMIT is tricky in sqlite, we cast in Python.
    sql = """
    SELECT m.id as movie_id, m.title, m.genre, m.release_year,
           COUNT(r.id) AS rental_count
    FROM movies m
    LEFT JOIN rentals r ON r.movie_id = m.id
    GROUP BY m.id
    ORDER BY rental_count DESC, m.title ASC
    LIMIT ?
    """
    cur.execute(sql, (limit,))                     # pass limit as parameter (tuple)
    results = cur.fetchall()                       # returns list of sqlite3.Row objects
    return results

# -------------------------
# Query: average rental duration
# -------------------------
def get_average_rental_duration(conn: sqlite3.Connection) -> float:
    """
    Compute average rental duration in days for completed rentals (return_date IS NOT NULL).
    We use SQLite's julianday() function which converts a date string to Julian day number,
    so difference of julianday(return_date) - julianday(rental_date) yields days as float.
    """
    cur = conn.cursor()
    sql = """
    SELECT AVG(julianday(return_date) - julianday(rental_date)) AS avg_days
    FROM rentals
    WHERE return_date IS NOT NULL
    """
    cur.execute(sql)
    row = cur.fetchone()
    # row["avg_days"] may be None if there are no returned rentals; handle that case
    avg_days = row["avg_days"] if row and row["avg_days"] is not None else None
    return avg_days

# -------------------------
# Utility: print nicely
# -------------------------
def print_top_movies(rows: List[sqlite3.Row]):
    """Print top movies in a human-friendly table."""
    print("\nTop rented movies:")
    print(f"{'Rank':<4} {'Title':<30} {'Genre':<10} {'Year':<6} {'Rentals':>7}")
    print("-" * 66)
    for i, r in enumerate(rows, start=1):
        # r is sqlite3.Row, accessible by column name
        title = (r["title"][:27] + "...") if len(r["title"]) > 30 else r["title"]
        print(f"{i:<4} {title:<30} {r['genre'] or '':<10} {str(r['release_year'] or ''):<6} {r['rental_count']:>7}")

def print_avg_duration(avg_days):
    """Print formatted average rental duration."""
    if avg_days is None:
        print("\nNo completed rentals found to compute average duration.")
    else:
        # format with two decimal places
        print(f"\nAverage rental duration (completed rentals): {avg_days:.2f} days")

# -------------------------
# Main program
# -------------------------
def main():
    # Connect (create DB file if needed)
    conn = get_connection(DB_FILE)

    # Create tables (idempotent)
    create_tables(conn)

    # Seed sample data if empty
    seed_sample_data(conn)

    # Query: get top 5 rented movies
    top5 = get_top_rented_movies(conn, limit=5)
    print_top_movies(top5)

    # Query: average rental duration (days)
    avg_days = get_average_rental_duration(conn)
    print_avg_duration(avg_days)

    # Close connection (good practice)
    conn.close()

# Standard Python entry point guard
if __name__ == "__main__":
    main()
