import os
import csv
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database(db_path=None):
    """Initialize the database schema"""
    if db_path is None:
        db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    
    conn = None
    try:
        # Ensure the data directory exists
        data_dir = os.path.dirname(db_path)
        os.makedirs(data_dir, exist_ok=True)
        
        # Try to set permissions on the directory, but don't fail if we can't
        try:
            os.chmod(data_dir, 0o777)
        except PermissionError:
            logger.warning("Could not set permissions on data directory - continuing anyway")
        
        # Connect to database with immediate write mode
        conn = sqlite3.connect(db_path, isolation_level=None)
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create tables with proper indexes
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_number TEXT PRIMARY KEY,
            order_date TEXT,
            order_paid_date TEXT,
            sales_associate TEXT,
            subtotal REAL,
            tip_total REAL
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS clubs (
            club_id TEXT PRIMARY KEY,
            club_signup_date TEXT,
            sales_associate TEXT
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS somm_scores (
            score_date TEXT,
            sales_associate TEXT,
            daily_score REAL,
            PRIMARY KEY (score_date, sales_associate)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            UNIQUE(key)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ref_table (
            date TEXT PRIMARY KEY,
            dow INTEGER,
            mon INTEGER,
            fisc_mon INTEGER,
            ttl_earn REAL,
            day_wght REAL
        )
        """)
        
        # Add indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_paid_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_clubs_date ON clubs(club_signup_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_scores_date ON somm_scores(score_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ref_date ON ref_table(date)")
        
        # Initialize default settings
        cursor.execute("""
        INSERT OR IGNORE INTO settings (key, value)
        VALUES 
            ('timezone', 'UTC'),
            ('year_type', 'calendar'),
            ('active_associates', '[]'),
            ('hidden_associates', '[]'),
            ('fiscal_year_start', '07-01'),
            ('fiscal_year_end', '06-30'),
            ('dark_mode', 'true'),
            ('show_tip_badges', 'true')
        """)
        
        # Commit all changes
        conn.commit()
        logger.info(f"Database initialized successfully at {db_path}")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    success = init_database(db_path)
    if not success:
        logger.error("Failed to initialize database")
        exit(1) 