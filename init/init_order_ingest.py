import requests
import sqlite3
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_order_ingest(start_date, end_date=None):
    """Initial ingest of order data from Commerce7 API
    
    Args:
        start_date (str): The start date in YYYY-MM-DD format
        end_date (str): Optional end date in YYYY-MM-DD format. If not provided, uses current date.
    """
    
    logger.info(f"Starting order ingestion from {start_date}")
    
    # Connect to database
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Set journal mode to WAL for better concurrent access
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    cursor = conn.cursor()

    # Create orders table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_number TEXT PRIMARY KEY,
            order_paid_date TEXT,
            subtotal REAL,
            tip_total REAL,
            sales_associate TEXT
        )
    ''')
    conn.commit()

    # API Configuration from environment variables
    tenant = os.getenv('C7_TENANT')
    auth_token = os.getenv('C7_AUTH_TOKEN')
    
    logger.info(f"Using tenant: {tenant}")
    if not tenant or not auth_token:
        logger.error("Missing required environment variables: C7_TENANT or C7_AUTH_TOKEN")
        return 0
        
    headers = {
        'Content-Type': 'application/json',
        'tenant': tenant,
        'Authorization': f"Basic {auth_token}"
    }
    
    # Initialize pagination
    cursor_value = 'start'
    total_inserted_orders = 0
    batch_size = 0  # Counter for current batch
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Using date range: {start_date} to {end_date}")

    try:
        while True:
            # Build the API URL with filters
            api_url = f'https://api.commerce7.com/v1/order?cursor={cursor_value}&channel=POS&orderPaidDate=btw:{start_date}|{end_date}'
            logger.info(f"Making API request to: {api_url}")
            
            # Make the API request
            try:
                response = requests.get(api_url, headers=headers)
                logger.info(f"API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    orders = data.get('orders', [])
                    logger.info(f"Retrieved {len(orders)} orders from API")
                    
                    for order in orders:
                        try:
                            vendor = order.get('externalOrderVendor')
                            if vendor == 'Tock':
                                continue
                            
                            subtotal = order.get('subTotal', 0) / 100 if order.get('subTotal') is not None else 0
                            if subtotal == 0:
                                continue
                            
                            # Extract fields
                            order_number = order.get('orderNumber')
                            order_paid_date = order.get('orderPaidDate')
                            tip_total = order.get('tipTotal', 0) / 100 if order.get('tipTotal') is not None else 0
                            sales_associate = order.get('salesAssociate', {}).get('name')

                            # Convert date format
                            if order_paid_date:
                                order_paid_date = order_paid_date.replace('T', ' ').replace('Z', '').split(' ')[0]

                            # Check if order exists
                            cursor.execute("SELECT 1 FROM orders WHERE order_number = ? LIMIT 1;", (order_number,))
                            if cursor.fetchone():
                                continue

                            # Insert into SQLite
                            cursor.execute("""
                                INSERT INTO orders (order_number, order_paid_date, subtotal, tip_total, sales_associate)
                                VALUES (?, ?, ?, ?, ?);
                            """, (
                                order_number,
                                order_paid_date,
                                subtotal,
                                tip_total,
                                sales_associate
                            ))
                            
                            batch_size += 1
                            
                            # Commit every 25 records
                            if batch_size >= 25:
                                conn.commit()
                                total_inserted_orders += batch_size
                                logger.info(f"Inserted {batch_size} orders into the database. Total so far: {total_inserted_orders}")
                                batch_size = 0
                                
                        except sqlite3.Error as e:
                            logger.error(f"SQLite error processing order {order.get('id')}: {e}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing order {order.get('id')}: {e}")
                            continue
                    
                    # Commit any remaining records in the batch
                    if batch_size > 0:
                        conn.commit()
                        total_inserted_orders += batch_size
                        logger.info(f"Inserted final {batch_size} orders into the database. Total so far: {total_inserted_orders}")
                        batch_size = 0
                    
                    cursor_value = data.get('cursor')
                    if not cursor_value:  # Exit if no more pages
                        break
                else:
                    logger.error(f'Failed to fetch data: {response.status_code} - {response.text}')
                    if response.status_code == 401:
                        logger.error("Authentication failed - check C7_TENANT and C7_AUTH_TOKEN")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected error during API request: {e}")
                break

    except Exception as e:
        logger.error(f"Critical error during order ingestion: {e}")
        conn.rollback()
    finally:
        try:
            # Close the database connection
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

    return total_inserted_orders