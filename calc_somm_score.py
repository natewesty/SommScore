import sqlite3
from datetime import datetime
from collections import defaultdict
import os
import json

def get_active_associates(conn):
    """Get active associates from the settings table."""
    result = conn.execute('SELECT value FROM settings WHERE key = "active_associates"').fetchone()
    return json.loads(result['value']) if result else []

def calculate_somm_scores(db_path, existing_conn=None, start_date=None):
    """
    Calculates daily SommScores and updates the somm_scores table.
    Score is based on current year performance only, combining:
    - Daily sales performance relative to team
    - Club signup bonuses
    - Day-specific weighting
    
    Args:
        db_path: Path to the SQLite database
        existing_conn: Optional existing database connection to use
        start_date: The start date of the current fiscal year (e.g. 2024-07-01)
    """
    if db_path is None:
        db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    
    conn = existing_conn
    should_close_conn = False
    
    if conn is None:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        should_close_conn = True
    
    cursor = conn.cursor()

    try:
        # Create table for daily SommScores if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS somm_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            score_date TEXT NOT NULL,
            sales_associate TEXT NOT NULL,
            daily_score REAL NOT NULL
        );
        """)
        conn.commit()

        # Get today's date for the current period end date
        today = datetime.now().strftime('%Y-%m-%d')

        # Get current year's orders (from start_date to today)
        cursor.execute("""
            SELECT
                date(order_paid_date) AS work_date,
                sales_associate,
                SUM(subtotal) AS total_revenue,
                COUNT(*) as order_count
            FROM orders
            WHERE date(order_paid_date) >= ? AND date(order_paid_date) <= ?
            GROUP BY date(order_paid_date), sales_associate
        """, (start_date, today))
        current_year_data = cursor.fetchall()
        
        # Store current year's data: { work_date: { sales_associate: (total_revenue, order_count) } }
        current_year_dict = defaultdict(dict)
        for row in current_year_data:
            wdate = row['work_date']
            associate = row['sales_associate']
            current_year_dict[wdate][associate] = (row['total_revenue'], row['order_count'])

        # Get club signups for current year
        cursor.execute("""
            SELECT
                date(club_signup_date) AS signup_date,
                sales_associate,
                COUNT(*) AS total_clubs
            FROM clubs
            WHERE date(club_signup_date) >= ? AND date(club_signup_date) <= ?
            GROUP BY date(club_signup_date), sales_associate
        """, (start_date, today))
        club_data = cursor.fetchall()
        club_dict = defaultdict(dict)
        for row in club_data:
            sdate = row['signup_date']
            associate = row['sales_associate']
            club_dict[sdate][associate] = row['total_clubs']

        # Get active associates from database
        active_associates = get_active_associates(conn)

        # Define day weights (1.0 for weekends, 1.5 for weekdays)
        def get_day_weight(date_str):
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            # Get day of week (0 = Monday, 6 = Sunday)
            dow = date_obj.weekday()
            # Return weight based on day
            return 1.0 if dow in [4, 5, 6] else 1.5  # 1.0 for Fri-Sun, 1.5 for Mon-Thu

        scores_by_date = defaultdict(list)
        skipped_entries = 0

        # Calculate scores for each day in the current year
        for current_date, associates_data in current_year_dict.items():
            day_weight = get_day_weight(current_date)
            
            # Get active associates who had orders on this day
            working_associates = {
                associate for associate, (_, order_count) in associates_data.items()
                if order_count > 0 and associate in active_associates
            }
            
            if not working_associates:
                continue
            
            # Calculate total team revenue for the day
            total_team_revenue = sum(revenue for associate, (revenue, _) 
                                   in associates_data.items() 
                                   if associate in working_associates)
            
            # Calculate average revenue per associate
            avg_revenue = total_team_revenue / len(working_associates) if working_associates else 0
            
            # Calculate scores for working associates
            for associate in working_associates:
                revenue, _ = associates_data[associate]
                
                # Base score from revenue performance relative to team average
                revenue_score = ((revenue - avg_revenue) / avg_revenue) * 50 if avg_revenue > 0 else 0
                
                # Apply day weight
                daily_score = revenue_score * day_weight
                
                # Add club signup bonus (50 points per signup)
                signup_count = club_dict.get(current_date, {}).get(associate, 0)
                daily_score += signup_count * 50
                
                scores_by_date[current_date].append((associate, daily_score))

        # Normalize scores and prepare for insertion
        somm_insert_rows = []
        for wdate, scores in scores_by_date.items():
            if not scores:
                continue
            
            daily_scores = [score for _, score in scores]
            min_score = min(daily_scores)
            max_score = max(daily_scores)
            
            score_range = max_score - min_score
            for associate, raw_score in scores:
                if score_range == 0:
                    normalized_score = 50.0
                else:
                    normalized_score = ((raw_score - min_score) / score_range) * 100
                    
                # Ensure score is between 0 and 100
                normalized_score = max(0, min(100, normalized_score))
                
                somm_insert_rows.append((wdate, associate, normalized_score))

        # After the insert, add this print statement
        print(f"Skipped {skipped_entries} entries due to missing sales associate.")

        # Clear existing records for these dates to avoid duplicates
        unique_dates = list({ row[0] for row in somm_insert_rows })
        cursor.execute(f"""
            DELETE FROM somm_scores
            WHERE score_date IN ({','.join(['?']*len(unique_dates))})
        """, unique_dates)
        conn.commit()

        # Insert new records
        cursor.executemany("""
            INSERT INTO somm_scores (score_date, sales_associate, daily_score)
            VALUES (?, ?, ?)
        """, somm_insert_rows)
        conn.commit()

        print(f"Inserted/updated {len(somm_insert_rows)} daily SommScores.")

        # Calculate and display aggregated YTD results
        cursor.execute("""
            SELECT
                sales_associate,
                COUNT(*) AS days_counted,
                ROUND(AVG(daily_score), 2) AS average_somm_score,
                ROUND(MIN(daily_score), 2) AS min_score,
                ROUND(MAX(daily_score), 2) AS max_score
            FROM somm_scores
            WHERE sales_associate IN ({})
            GROUP BY sales_associate
            ORDER BY average_somm_score DESC
        """.format(','.join(['?'] * len(active_associates))), list(active_associates))
        aggregate_results = cursor.fetchall()

        print("\n----- Normalized SommScores (0-100 scale) -----")
        for row in aggregate_results:
            print(f"Associate: {row['sales_associate']}, "
                  f"Days Counted: {row['days_counted']}, "
                  f"Avg Score: {row['average_somm_score']:.1f}, "
                  f"Range: {row['min_score']:.1f}-{row['max_score']:.1f}")

    except Exception as e:
        print(f"Error calculating scores: {e}")
        if conn:
            conn.rollback()
    finally:
        if should_close_conn and conn:
            conn.close()


if __name__ == "__main__":
    # Example usage
    db_file_path = "path/to/your/database.db"
    calculate_somm_scores(db_file_path)
