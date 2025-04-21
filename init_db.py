import os
import csv
import sqlite3
import logging

# Configure logging with more detail
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_database(db_path=None):
    """Initialize the database schema for demo data"""
    if db_path is None:
        db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    
    logger.info(f"Starting database initialization at path: {db_path}")
    conn = None
    try:
        # Ensure the data directory exists
        data_dir = os.path.dirname(db_path)
        logger.debug(f"Ensuring data directory exists: {data_dir}")
        os.makedirs(data_dir, exist_ok=True)
        
        # Try to set permissions on the directory, but don't fail if we can't
        try:
            os.chmod(data_dir, 0o777)
            logger.debug(f"Set permissions on data directory: {data_dir}")
        except PermissionError:
            logger.warning("Could not set permissions on data directory - continuing anyway")
        
        # Connect to database with immediate write mode
        logger.debug("Connecting to database...")
        conn = sqlite3.connect(db_path, isolation_level=None)
        cursor = conn.cursor()
        
        # Enable foreign keys
        logger.debug("Enabling foreign keys...")
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create tables with proper indexes
        logger.debug("Creating tables...")
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
        logger.debug("Created orders table")
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS clubs (
            club_id TEXT PRIMARY KEY,
            club_signup_date TEXT,
            sales_associate TEXT
        )
        """)
        logger.debug("Created clubs table")
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS somm_scores (
            score_date TEXT,
            sales_associate TEXT,
            daily_score REAL,
            PRIMARY KEY (score_date, sales_associate)
        )
        """)
        logger.debug("Created somm_scores table")
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            UNIQUE(key)
        )
        """)
        logger.debug("Created settings table")
        
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
        logger.debug("Created ref_table table")
        
        # Add indexes for performance
        logger.debug("Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_paid_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_clubs_date ON clubs(club_signup_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_scores_date ON somm_scores(score_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ref_date ON ref_table(date)")
        logger.debug("Created all indexes")
        
        # Initialize default settings
        logger.debug("Initializing default settings...")
        cursor.execute("""
        INSERT OR IGNORE INTO settings (key, value)
        VALUES 
            ('timezone', 'UTC'),
            ('year_type', 'calendar'),
            ('active_associates', '[]'),
            ('hidden_associates', '[]'),
            ('fiscal_year_start', '01-01'),
            ('fiscal_year_end', '12-31'),
            ('dark_mode', 'true'),
            ('show_tip_badges', 'true')
        """)
        logger.debug("Initialized default settings")
        
        # Verify tables were created
        logger.debug("Verifying table creation...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        logger.info(f"Found tables: {[table[0] for table in tables]}")
        
        # Commit all changes
        conn.commit()
        logger.info(f"Database initialized successfully at {db_path}")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error during initialization: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error during initialization: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

if __name__ == "__main__":
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    success = init_database(db_path)
    if not success:
        logger.error("Failed to initialize database")
        exit(1) 