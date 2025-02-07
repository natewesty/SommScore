import os
import sqlite3
import schedule
import time
import logging
from datetime import datetime
from typing import Optional, Tuple
from init_db import init_database
from init.init_order_ingest import init_order_ingest
from init.init_club_ingest import init_club_ingest
from calc_somm_score import calculate_somm_scores

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Ensure all handlers use the same format
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
for handler in logger.handlers:
    handler.setFormatter(formatter)

def ensure_database_initialized(db_path: str = None) -> None:
    """Ensure database exists and is initialized"""
    if db_path is None:
        db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    
    if not os.path.exists(db_path):
        logger.info("Database not found. Initializing...")
        init_database(db_path)
    
    # Test connection and table existence
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("SELECT 1 FROM settings LIMIT 1")
        conn.close()
    except sqlite3.Error:
        logger.info("Database schema not initialized. Creating tables...")
        init_database(db_path)

def get_last_update_time(db_path: str = None) -> Tuple[str, str]:
    """Get the last update times from the settings table"""
    if db_path is None:
        db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get last order update time
    cursor.execute("SELECT value FROM settings WHERE key = 'last_order_update'")
    last_order = cursor.fetchone()
    
    # Get last club update time
    cursor.execute("SELECT value FROM settings WHERE key = 'last_club_update'")
    last_club = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    return (
        last_order['value'] if last_order else '2024-01-01',
        last_club['value'] if last_club else '2024-01-01'
    )

def update_data(start_date: str = None, end_date: str = None) -> None:
    """Update data from Commerce7 API and calculate new SommScores.
    
    Args:
        start_date (str, optional): Start date for the update in YYYY-MM-DD format.
        end_date (str, optional): End date for the update in YYYY-MM-DD format.
    """
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    conn = None
    
    try:
        logger.info("=== Starting Update Process ===")
        logger.info(f"Database path: {db_path}")
        
        # Ensure database is properly initialized
        logger.info("Checking database initialization...")
        ensure_database_initialized(db_path)
        logger.info("Database initialization check complete")
        
        # Get update range
        current_time = datetime.now()
        current_time_str = current_time.strftime('%Y-%m-%d')
        
        if not start_date:
            # If no start date provided, use last update time
            logger.info("No start date provided, fetching last update time...")
            start_date, _ = get_last_update_time(db_path)
            logger.info(f"Using last update time as start date: {start_date}")
        
        if not end_date:
            end_date = current_time_str
            logger.info(f"Using current date as end date: {end_date}")
        
        logger.info(f"Update range: {start_date} to {end_date}")
        
        # Ingest new data using init functions
        logger.info("=== Starting Commerce7 API Data Fetch ===")
        
        logger.info(f"Fetching orders from Commerce7 API for period {start_date} to {end_date}...")
        new_orders = init_order_ingest(
            start_date=start_date,
            end_date=end_date
        )
        logger.info(f"Order fetch complete - Added {new_orders} new orders")
        
        logger.info(f"Fetching clubs from Commerce7 API for period {start_date} to {end_date}...")
        new_clubs = init_club_ingest(
            start_date=start_date,
            end_date=end_date
        )
        logger.info(f"Club fetch complete - Added {new_clubs} new clubs")
        
        logger.info("=== API Data Fetch Complete ===")
        
        # Only update the last update time if we're running up to current date
        if end_date == current_time_str:
            logger.info("Updating last update timestamps...")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("UPDATE settings SET value = ? WHERE key = 'last_order_update'", (end_date,))
            cursor.execute("UPDATE settings SET value = ? WHERE key = 'last_club_update'", (end_date,))
            conn.commit()
            cursor.close()
            conn.close()
            conn = None
            logger.info("Last update timestamps updated successfully")
        else:
            logger.info("Skipping last update timestamp update (not a current date update)")
        
        logger.info(f"Data update complete - Added {new_orders} orders and {new_clubs} clubs")
        
        # Calculate updated scores
        logger.info("=== Starting Score Calculation ===")
        try:
            # Get year type and determine start date for score calculation
            logger.info("Determining score calculation period...")
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            year_type = conn.execute('SELECT value FROM settings WHERE key = "year_type"').fetchone()['value']
            logger.info(f"Year type setting: {year_type}")
            
            if year_type == 'fiscal':
                fiscal_start = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_start"').fetchone()['value']
                fiscal_start_date = datetime.strptime(fiscal_start, '%Y-%m-%d')
                logger.info(f"Fiscal year start date: {fiscal_start}")
                
                # Calculate current fiscal year
                if current_time >= fiscal_start_date.replace(year=current_time.year):
                    current_fiscal_year = current_time.year
                else:
                    current_fiscal_year = current_time.year - 1
                    
                score_start_date = fiscal_start_date.replace(year=current_fiscal_year).strftime('%Y-%m-%d')
                logger.info(f"Using fiscal year start date for scores: {score_start_date}")
            else:
                # Calendar year - start from January 1st
                score_start_date = current_time.replace(month=1, day=1).strftime('%Y-%m-%d')
                logger.info(f"Using calendar year start date for scores: {score_start_date}")
            
            conn.close()
            conn = None
            
            # Calculate scores with the correct start date
            logger.info(f"Calculating scores from {score_start_date} to present...")
            calculate_somm_scores(db_path, start_date=score_start_date)
            logger.info("Score calculation complete")
            
        except Exception as e:
            logger.error(f"Error during score calculation: {str(e)}")
            logger.error("Score calculation failed - see error above")
            raise
            
        logger.info("=== Update Process Complete ===")
            
    except Exception as e:
        logger.error(f"Error during update process: {str(e)}")
        if conn:
            try:
                conn.rollback()
                logger.info("Database connection rolled back")
            except:
                logger.error("Failed to rollback database connection")
                pass
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed")
            except:
                logger.error("Failed to close database connection")
                pass

def run_scheduler() -> None:
    """Run the scheduler with error handling."""
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error in scheduler: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying

if __name__ == "__main__":
    logger.info("Starting daily update scheduler...")
    
    # Get database path from environment or use default
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Schedule the update to run at 1am daily
    schedule.every().day.at("01:00").do(update_data)
    logger.info("Scheduled daily update for 1:00 AM")
    
    # Run the scheduler
    run_scheduler()