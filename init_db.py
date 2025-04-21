import os
import csv
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database(db_path):
    """Initialize the SQLite database with all required tables."""
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create settings table first
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        
        # Create orders table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                order_number TEXT NOT NULL,
                order_paid_date DATE NOT NULL,
                subtotal REAL NOT NULL,
                tip_total REAL NOT NULL,
                sales_associate TEXT NOT NULL
            )
        ''')
        
        # Create clubs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clubs (
                id TEXT PRIMARY KEY,
                club_name TEXT NOT NULL,
                club_signup_date DATE NOT NULL,
                sales_associate TEXT NOT NULL
            )
        ''')
        
        # Create ref_table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ref_table (
                date DATE PRIMARY KEY,
                dow INTEGER NOT NULL,
                mon INTEGER NOT NULL,
                fisc_mon INTEGER NOT NULL,
                ttl_earn REAL NOT NULL,
                day_wght REAL NOT NULL
            )
        ''')
        
        # Create somm_scores table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS somm_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                score_date DATE NOT NULL,
                sales_associate TEXT NOT NULL,
                daily_score REAL NOT NULL,
                UNIQUE(score_date, sales_associate)
            )
        ''')
        
        # Commit changes
        conn.commit()
        logger.info(f"Database initialized successfully at {db_path}")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    init_database(db_path) 