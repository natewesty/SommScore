import sqlite3
import os
import random
from datetime import datetime, timedelta
import json
from init_db import init_database
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_fake_data():
    """Generate fake data for demonstration purposes."""
    logger.info("Starting fake data generation...")
    
    # Use the same database path as the main application
    db_path = os.getenv('DB_PATH', '/data/commerce7.db')
    logger.info(f"Using database path: {db_path}")
    
    try:
        # Initialize the database
        init_database(db_path)
        
        # Create a list of fake sales associates
        sales_associates = [
            "Sarah Johnson", "Michael Chen", "Emily Rodriguez",
            "David Kim", "Jessica Martinez", "Robert Wilson",
            "Jennifer Lee", "Christopher Brown", "Amanda Taylor",
            "Daniel Garcia"
        ]
        
        # Update active associates in settings
        conn = get_db_connection()
        conn.execute("UPDATE settings SET value = ? WHERE key = 'active_associates'",
                    (json.dumps(sales_associates),))
        conn.commit()
        logger.info(f"Set active associates: {sales_associates}")
        
        # Generate dates for the past year
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        current_date = start_date
        
        logger.info("Generating fake orders...")
        # Generate orders
        order_id = 1
        while current_date <= end_date:
            # Skip weekends (Saturday and Sunday)
            if current_date.weekday() < 5:  # 0-4 is Monday-Friday
                # Generate 5-15 orders per day
                num_orders = random.randint(5, 15)
                for _ in range(num_orders):
                    sales_associate = random.choice(sales_associates)
                    subtotal = round(random.uniform(50, 500), 2)
                    tip_total = round(subtotal * random.uniform(0.15, 0.25), 2)
                    
                    cursor.execute("""
                        INSERT INTO orders (order_number, order_paid_date, subtotal, tip_total, sales_associate)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        f"ORD-{order_id:06d}",
                        current_date.strftime('%Y-%m-%d'),
                        subtotal,
                        tip_total,
                        sales_associate
                    ))
                    order_id += 1
            
            current_date += timedelta(days=1)
        
        logger.info("Generating fake club memberships...")
        # Generate club memberships
        club_id = 1
        current_date = start_date
        while current_date <= end_date:
            # Generate 0-3 club signups per day
            num_clubs = random.randint(0, 3)
            for _ in range(num_clubs):
                sales_associate = random.choice(sales_associates)
                
                cursor.execute("""
                    INSERT INTO clubs (club_id, club_signup_date, sales_associate)
                    VALUES (?, ?, ?)
                """, (
                    f"club_{club_id}",
                    current_date.strftime('%Y-%m-%d'),
                    sales_associate
                ))
                club_id += 1
            
            current_date += timedelta(days=1)
        
        logger.info("Generating reference data...")
        # Generate reference data
        current_date = start_date
        while current_date <= end_date:
            # Calculate day of week (0 = Monday, 6 = Sunday)
            dow = current_date.weekday()
            # Convert to 1-7 format where 1 = Sunday
            dow = (dow + 2) % 7 if dow != 6 else 1
            
            # Calculate month (1-12)
            mon = current_date.month
            
            # Set default weights based on day of week
            if dow == 1:  # Sunday
                day_wght = 1.0
            elif dow == 6:  # Friday
                day_wght = 1.0
            elif dow == 7:  # Saturday
                day_wght = 1.0
            else:  # Monday-Thursday
                day_wght = 1.5
            
            # Generate total earnings for the day
            ttl_earn = round(random.uniform(1000, 5000), 2)
            
            cursor.execute("""
                INSERT INTO ref_table (date, dow, mon, fisc_mon, ttl_earn, day_wght)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                current_date.strftime('%Y-%m-%d'),
                dow,
                mon,
                mon,  # Using calendar month for fiscal month in this example
                ttl_earn,
                day_wght
            ))
            
            current_date += timedelta(days=1)
        
        # Commit all changes
        conn.commit()
        
        logger.info("Calculating SommScores...")
        # Calculate SommScores
        from calc_somm_score import calculate_somm_scores
        calculate_somm_scores(db_path, conn, start_date.strftime('%Y-%m-%d'))
        
        logger.info("Fake data generation complete!")
        return True
        
    except Exception as e:
        logger.error(f"Error generating fake data: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    generate_fake_data() 