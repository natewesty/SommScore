import requests
import sqlite3
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def init_club_ingest(start_date, end_date=None):
    """Initial ingest of club membership data from Commerce7 API
    
    Args:
        start_date (str): The start date in YYYY-MM-DD format
        end_date (str): Optional end date in YYYY-MM-DD format. If not provided, uses current date.
        
    Returns:
        int: Number of new club memberships added to the database
    """
    
    # Connect to database
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create clubs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clubs (
            id TEXT PRIMARY KEY,
            club_name TEXT,
            club_signup_date TEXT,
            sales_associate TEXT
        )
    ''')

    # API Configuration from environment variables
    headers = {
        'Content-Type': 'application/json',
        'tenant': os.getenv('C7_TENANT'),
        'Authorization': f"Basic {os.getenv('C7_AUTH_TOKEN')}"
    }
    payload = {}

    # Initialize the cursor pagination with date filter
    cursor_value = 'start'
    total_inserted_clubs = 0
    
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    while True:
        # Build the API URL with filters
        api_url = f'https://api.commerce7.com/v1/club-membership?cursor={cursor_value}&signupDate=btw:{start_date}|{end_date}'
        
        # Make the API request
        response = requests.request("GET", api_url, headers=headers, data=payload)
        
        if response.status_code == 200:
            data = response.json()
            clubs = data.get('clubMemberships', [])
            
            for club in clubs:
                
                club_id = club['id']
                club_name = club.get('club', {}).get('title')
                club_signup_date = club.get('signupDate')
                sales_associate = club.get('salesAssociate', {}).get('name')
                
                if sales_associate is None or sales_associate == 'Eric Molinatti':
                    continue

                # Convert date format
                if club_signup_date:
                    club_signup_date = club_signup_date.replace('T', ' ').replace('Z', '').split(' ')[0]

                # Check if club exists
                cursor.execute("SELECT 1 FROM clubs WHERE id = ? LIMIT 1;", (club_id,))
                if cursor.fetchone():
                    continue

                # Insert into SQLite (note the ? placeholders instead of %s)
                insert_query = """
                    INSERT INTO clubs (id, club_name, club_signup_date, sales_associate)
                    VALUES (?, ?, ?, ?);
                """
                cursor.execute(insert_query, (
                    club_id,
                    club_name,
                    club_signup_date,
                    sales_associate
                ))
            
            conn.commit()
            total_inserted_clubs += len(clubs)
            print(f"Inserted {len(clubs)} clubs into the database. Total so far: {total_inserted_clubs}")
            
            cursor_value = data.get('cursor')
            if not cursor_value:  # Exit if no more pages
                break

        else:
            print(f'Failed to fetch data: {response.status_code} - {response.text}')
            break

    # Close the database connection
    cursor.close()
    conn.close()
    
    return total_inserted_clubs