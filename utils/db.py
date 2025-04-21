import sqlite3
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a database connection with consistent path."""
    # Always use the absolute path from environment variable
    db_path = os.getenv('DB_PATH', '/data/commerce7.db')
    logger.info(f"Connecting to database at: {db_path}")
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn 