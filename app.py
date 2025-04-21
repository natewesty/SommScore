from flask import Flask, render_template, request, redirect, url_for, jsonify
import sqlite3
from datetime import datetime, timedelta
import json
from dateutil.relativedelta import relativedelta
import os
from init_db import init_database
import logging
import threading
import time
from queue import Queue
from init.init_order_ingest import init_order_ingest
from init.init_club_ingest import init_club_ingest
from daily_update import update_data
import schedule
from utils.timezone_helper import get_timezones_by_region, get_current_timezone, validate_timezone, convert_to_utc
from generate_fake_data import generate_fake_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global scheduler thread
scheduler_thread = None

# Check if we're in demo mode
DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

# Add global initialization status and lock
initialization_complete = False
initialization_lock = threading.Lock()

def run_scheduler():
    """Run the scheduler in a background thread."""
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error in scheduler: {e}")
            time.sleep(300)

def init_scheduler():
    """Initialize the daily update scheduler."""
    global scheduler_thread
    
    # Clear existing jobs
    schedule.clear()
    
    try:
        # Get timezone from settings
        db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
        timezone = get_current_timezone(db_path)
        
        # Convert 1 AM local time to UTC
        utc_time = convert_to_utc("01:00", timezone)
        logger.info(f"Scheduling daily update for {utc_time} UTC (1:00 AM {timezone})")
        
        # Schedule the update
        schedule.every().day.at(utc_time).do(update_data)
        
        # Start scheduler thread if not running
        if scheduler_thread is None or not scheduler_thread.is_alive():
            scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
            scheduler_thread.start()
            logger.info("Scheduler thread started")
            
    except Exception as e:
        logger.error(f"Error initializing scheduler: {e}")
        logger.info("Falling back to UTC for scheduler")
        schedule.every().day.at("01:00").do(update_data)

def restart_scheduler():
    """Restart the scheduler with updated timezone."""
    global scheduler_thread
    
    # Clear existing jobs
    schedule.clear()
    
    # Stop existing thread if running
    if scheduler_thread and scheduler_thread.is_alive():
        scheduler_thread = None
    
    # Initialize new scheduler
    init_scheduler()

# Add global variables for tracking setup progress
setup_progress = {
    'status': 'not_started',
    'message': 'Setup not started',
    'error': None
}

def get_db_connection():
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def init_settings_table():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    
    # Add default settings
    conn.execute('''
        INSERT OR IGNORE INTO settings (key, value) 
        VALUES 
            ('year_type', 'calendar'),
            ('active_associates', '[]'),
            ('hidden_associates', '[]'),
            ('fiscal_year_start', '07-01'),
            ('fiscal_year_end', '06-30'),
            ('dark_mode', 'true'),
            ('show_tip_badges', 'true'),
            ('timezone', 'America/Los_Angeles')
    ''')
    conn.commit()
    conn.close()

def get_all_associates():
    conn = get_db_connection()
    associates = conn.execute('''
        SELECT DISTINCT sales_associate 
        FROM (
            SELECT sales_associate FROM orders 
            WHERE sales_associate IS NOT NULL
            UNION
            SELECT sales_associate FROM clubs
            WHERE sales_associate IS NOT NULL
        )
        ORDER BY sales_associate
    ''').fetchall()
    conn.close()
    return [a['sales_associate'] for a in associates]

def get_active_associates():
    """Get list of active associates from settings table."""
    db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
    
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    try:
        conn = get_db_connection()
        result = conn.execute('SELECT value FROM settings WHERE key = "active_associates"').fetchone()
        conn.close()
        return json.loads(result['value']) if result else []
    except Exception as e:
        logger.error(f"Error getting active associates: {e}")
        return []

def get_hidden_associates():
    """Get list of hidden associates from settings table."""
    try:
        conn = get_db_connection()
        result = conn.execute('SELECT value FROM settings WHERE key = "hidden_associates"').fetchone()
        conn.close()
        return json.loads(result['value']) if result else []
    except Exception as e:
        logger.error(f"Error getting hidden associates: {e}")
        return []

def get_year_type():
    conn = get_db_connection()
    result = conn.execute('SELECT value FROM settings WHERE key = "year_type"').fetchone()
    conn.close()
    return result['value'] if result else 'calendar'

def update_fiscal_year_if_needed():
    """Check and update fiscal year dates if the end date has passed."""
    conn = get_db_connection()
    
    # Get current year type and fiscal dates
    year_type = conn.execute('SELECT value FROM settings WHERE key = "year_type"').fetchone()['value']
    
    if year_type == 'fiscal':
        fiscal_end = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_end"').fetchone()['value']
        fiscal_start = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_start"').fetchone()['value']
        
        try:
            end_date = datetime.strptime(fiscal_end, '%Y-%m-%d')
            start_date = datetime.strptime(fiscal_start, '%Y-%m-%d')
            current_date = datetime.now()
            
            # If we've passed the end date, increment both dates by a year
            if current_date > end_date:
                new_start = start_date.replace(year=start_date.year + 1)
                new_end = end_date.replace(year=end_date.year + 1)
                
                # Update the database with new dates
                conn.execute('UPDATE settings SET value = ? WHERE key = "fiscal_year_start"', 
                           (new_start.strftime('%Y-%m-%d'),))
                conn.execute('UPDATE settings SET value = ? WHERE key = "fiscal_year_end"', 
                           (new_end.strftime('%Y-%m-%d'),))
                conn.commit()
        except (ValueError, TypeError):
            # Handle invalid date formats
            pass
    
    conn.close()

def is_initialized():
    """Check if the application has been initialized with start dates"""
    conn = get_db_connection()
    result = conn.execute('''
        SELECT COUNT(*) as count 
        FROM settings 
        WHERE key IN ('last_order_update', 'last_club_update')
    ''').fetchone()
    conn.close()
    return result['count'] == 2

def generate_ref_data(start_date, end_date):
    """Generate reference table data for the given date range."""
    ref_data = []
    current_date = start_date
    
    while current_date <= end_date:
        # Calculate day of week (0 = Monday, 6 = Sunday)
        dow = current_date.weekday()
        # Convert to 1-7 format where 1 = Sunday
        dow = (dow + 2) % 7 if dow != 6 else 1
        
        # Calculate month (1-12)
        mon = current_date.month
        
        # Calculate fiscal month based on fiscal year start
        conn = get_db_connection()
        year_type = conn.execute('SELECT value FROM settings WHERE key = "year_type"').fetchone()['value']
        
        if year_type == 'fiscal':
            fiscal_start = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_start"').fetchone()['value']
            fiscal_start_date = datetime.strptime(fiscal_start, '%Y-%m-%d')
            # Calculate months since fiscal year start
            months_diff = (current_date.month - fiscal_start_date.month) % 12
            fisc_mon = months_diff + 1
        else:
            fisc_mon = mon
            
        conn.close()
        
        # Get total earnings for this date from orders
        conn = get_db_connection()
        total_earnings = conn.execute('''
            SELECT COALESCE(SUM(subtotal), 0) as total
            FROM orders
            WHERE date(order_paid_date) = ?
        ''', (current_date.strftime('%Y-%m-%d'),)).fetchone()['total']
        conn.close()
        
        # Set default weights based on day of week
        if dow == 1:  # Sunday
            day_wght = 1.0
        elif dow == 6:  # Friday
            day_wght = 1.0
        elif dow == 7:  # Saturday
            day_wght = 1.0
        else:  # Monday-Thursday
            day_wght = 1.5
            
        ref_data.append({
            'date': current_date.strftime('%Y-%m-%d'),
            'dow': dow,
            'mon': mon,
            'fisc_mon': fisc_mon,
            'ttl_earn': total_earnings,
            'day_wght': day_wght
        })
        
        current_date += timedelta(days=1)
    
    return ref_data

def normalize_ref_data(conn, ref_start_date, ref_end_date):
    """
    Normalize the reference data by:
    1. Getting total earnings for each day in the reference period
    2. Calculating monthly averages by day of week
    3. Applying these averages back to the ref_table
    4. Handling missing data with fallback calculations
    """
    # First, get the daily totals from orders
    daily_totals = conn.execute('''
        SELECT 
            date(order_paid_date) as order_date,
            SUM(subtotal) as daily_total
        FROM orders 
        WHERE order_paid_date >= ? AND order_paid_date < ?
        GROUP BY date(order_paid_date)
    ''', (ref_start_date.strftime('%Y-%m-%d'), ref_end_date.strftime('%Y-%m-%d'))).fetchall()
    
    # Update ref_table with daily totals
    for day in daily_totals:
        conn.execute('''
            UPDATE ref_table 
            SET ttl_earn = ?
            WHERE date = ?
        ''', (day['daily_total'], day['order_date']))
    
    # Calculate monthly averages by day of week
    monthly_averages = conn.execute('''
        SELECT 
            mon,
            dow,
            AVG(ttl_earn) as avg_earn,
            COUNT(*) as day_count
        FROM ref_table
        WHERE ttl_earn > 0
        GROUP BY mon, dow
    ''').fetchall()
    
    # Calculate overall averages by day of week (fallback for missing data)
    overall_averages = conn.execute('''
        SELECT 
            dow,
            AVG(ttl_earn) as avg_earn,
            COUNT(*) as day_count
        FROM ref_table
        WHERE ttl_earn > 0
        GROUP BY dow
    ''').fetchall()
    
    # Create lookup dictionaries
    monthly_lookup = {(row['mon'], row['dow']): {
        'avg': row['avg_earn'],
        'count': row['day_count']
    } for row in monthly_averages}
    
    overall_lookup = {row['dow']: row['avg_earn'] for row in overall_averages}
    
    # Function to get adjacent months' average
    def get_adjacent_months_avg(month, dow):
        prev_month = month - 1 if month > 1 else 12
        next_month = month + 1 if month < 12 else 1
        
        adjacent_avg = conn.execute('''
            SELECT AVG(ttl_earn) as adj_avg
            FROM ref_table
            WHERE ttl_earn > 0
            AND dow = ?
            AND mon IN (?, ?)
        ''', (dow, prev_month, next_month)).fetchone()
        
        return adjacent_avg['adj_avg'] if adjacent_avg['adj_avg'] is not None else None
    
    # Apply the normalized values with fallbacks for missing data
    for mon in range(1, 13):
        for dow in range(1, 8):
            # Try to get the monthly average first
            monthly_data = monthly_lookup.get((mon, dow))
            
            if monthly_data and monthly_data['count'] >= 2:
                # Use monthly average if we have at least 2 data points
                avg_value = monthly_data['avg']
            else:
                # Try adjacent months
                adj_avg = get_adjacent_months_avg(mon, dow)
                if adj_avg is not None:
                    avg_value = adj_avg
                else:
                    # Fall back to overall day-of-week average
                    avg_value = overall_lookup.get(dow, 0.0)
            
            conn.execute('''
                UPDATE ref_table
                SET ttl_earn = ?
                WHERE mon = ? AND dow = ?
            ''', (avg_value, mon, dow))
    
    # Log normalization results
    normalization_stats = conn.execute('''
        SELECT 
            mon,
            COUNT(*) as total_days,
            COUNT(CASE WHEN ttl_earn > 0 THEN 1 END) as days_with_data,
            ROUND(AVG(ttl_earn), 2) as avg_earnings
        FROM ref_table
        GROUP BY mon
        ORDER BY mon
    ''').fetchall()
    
    print("\nNormalization Results:")
    print("Month | Total Days | Days with Data | Avg Earnings")
    print("-" * 50)
    for stat in normalization_stats:
        print(f"{stat['mon']:5d} | {stat['total_days']:10d} | {stat['days_with_data']:13d} | ${stat['avg_earnings']:11.2f}")

def process_setup(year_type, start_date, timezone, progress_dict):
    """Process setup in a background thread"""
    conn = None
    try:
        progress_dict['status'] = 'initializing'
        progress_dict['message'] = 'Initializing database...'
        
        # Initialize the database
        db_path = os.getenv('DB_PATH', os.path.join('data', 'commerce7.db'))
        init_database(db_path)
        
        # Create a single database connection for all operations
        conn = get_db_connection()
        
        # Calculate reference data period
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        
        # Reference period should be exactly one year before the start date
        ref_start_date = start_date_obj - timedelta(days=366)
        ref_end_date = start_date_obj - timedelta(days=1)  # End the day before the start date
        
        logger.info(f"Calculated reference period: {ref_start_date.strftime('%Y-%m-%d')} to {ref_end_date.strftime('%Y-%m-%d')}")
        
        # First, ingest reference period data
        progress_dict['status'] = 'fetching_reference_orders'
        progress_dict['message'] = 'Fetching reference period order data...'
        
        # Ingest orders for reference period
        orders_added = init_order_ingest(
            ref_start_date.strftime('%Y-%m-%d'),
            ref_end_date.strftime('%Y-%m-%d')
        )
        logger.info(f"Reference period orders added: {orders_added}")
        
        progress_dict['status'] = 'fetching_reference_clubs'
        progress_dict['message'] = 'Fetching reference period club data...'
        
        # Ingest clubs for reference period
        clubs_added = init_club_ingest(
            ref_start_date.strftime('%Y-%m-%d'),
            ref_end_date.strftime('%Y-%m-%d')
        )
        logger.info(f"Reference period clubs added: {clubs_added}")
        
        # Now ingest current fiscal year data
        progress_dict['status'] = 'fetching_current_orders'
        progress_dict['message'] = 'Fetching current fiscal year order data...'
        
        # Ingest orders for current period
        current_orders = init_order_ingest(
            start_date,
            datetime.now().strftime('%Y-%m-%d')
        )
        logger.info(f"Current period orders added: {current_orders}")
        
        progress_dict['status'] = 'fetching_current_clubs'
        progress_dict['message'] = 'Fetching current fiscal year club data...'
        
        # Ingest clubs for current period
        current_clubs = init_club_ingest(
            start_date,
            datetime.now().strftime('%Y-%m-%d')
        )
        logger.info(f"Current period clubs added: {current_clubs}")
        
        # Get all associates and set them as active
        progress_dict['status'] = 'setting_active_associates'
        progress_dict['message'] = 'Setting up active associates...'
        
        all_associates = get_all_associates()
        if all_associates:
            conn.execute("UPDATE settings SET value = ? WHERE key = 'active_associates'", 
                        (json.dumps(all_associates),))
            logger.info(f"Set active associates: {all_associates}")
        
        progress_dict['status'] = 'generating_reference'
        progress_dict['message'] = 'Generating reference data...'
        
        # Generate reference data
        ref_data = generate_ref_data(ref_start_date, ref_end_date)
        conn.execute("DELETE FROM ref_table")
        conn.executemany("""
            INSERT INTO ref_table (date, dow, mon, fisc_mon, ttl_earn, day_wght)
            VALUES (:date, :dow, :mon, :fisc_mon, :ttl_earn, :day_wght)
        """, ref_data)
        
        progress_dict['status'] = 'normalizing'
        progress_dict['message'] = 'Normalizing reference data...'
        
        normalize_ref_data(conn, ref_start_date, ref_end_date)
        
        progress_dict['status'] = 'calculating_scores'
        progress_dict['message'] = 'Calculating initial SommScores...'
        
        # Import and run score calculation with the existing connection
        from calc_somm_score import calculate_somm_scores
        calculate_somm_scores(db_path, conn, start_date)
        
        # Update settings
        conn.execute("""
            INSERT OR REPLACE INTO settings (key, value)
            VALUES ('last_order_update', ?),
                   ('last_club_update', ?),
                   ('year_type', ?),
                   ('timezone', ?)
        """, (start_date, start_date, year_type, timezone))
        
        # Add fiscal year settings if needed
        if year_type == 'fiscal':
            fiscal_start_date = datetime.strptime(start_date, '%Y-%m-%d')
            fiscal_end_date = fiscal_start_date.replace(year=fiscal_start_date.year + 1) - timedelta(days=1)
            conn.execute("""
                INSERT OR REPLACE INTO settings (key, value)
                VALUES ('fiscal_year_start', ?),
                       ('fiscal_year_end', ?)
            """, (start_date, fiscal_end_date.strftime('%Y-%m-%d')))
        
        conn.commit()
        
        progress_dict['status'] = 'complete'
        progress_dict['message'] = 'Setup complete!'
        
    except Exception as e:
        progress_dict['status'] = 'error'
        progress_dict['message'] = str(e)
        progress_dict['error'] = str(e)
        logger.error(f"Setup error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if not initialization_complete:
        return render_template('initializing.html'), 503
    conn = get_db_connection()
    
    if request.method == 'POST':
        # Handle timezone update
        new_timezone = request.form.get('timezone')
        if new_timezone and validate_timezone(new_timezone):
            conn.execute('UPDATE settings SET value = ? WHERE key = "timezone"', (new_timezone,))
            # Restart scheduler with new timezone
            restart_scheduler()
        
        # Handle active and hidden associates
        active_associates = json.loads(request.form.get('active_associates', '[]'))
        hidden_associates = json.loads(request.form.get('hidden_associates', '[]'))
        
        conn.execute('UPDATE settings SET value = ? WHERE key = "active_associates"', 
                    (json.dumps(active_associates),))
        conn.execute('UPDATE settings SET value = ? WHERE key = "hidden_associates"', 
                    (json.dumps(hidden_associates),))
        
        # Handle year type and fiscal dates
        year_type = request.form.get('year_type', 'calendar')
        conn.execute('UPDATE settings SET value = ? WHERE key = "year_type"', (year_type,))
        
        if year_type == 'fiscal':
            fiscal_start = request.form.get('fiscal_start')
            fiscal_end = request.form.get('fiscal_end')
            conn.execute('UPDATE settings SET value = ? WHERE key = "fiscal_year_start"', 
                        (fiscal_start,))
            conn.execute('UPDATE settings SET value = ? WHERE key = "fiscal_year_end"', 
                        (fiscal_end,))
        
        # Handle display options
        dark_mode = 'true' if request.form.get('dark_mode') else 'false'
        show_tip_badges = 'true' if request.form.get('show_tip_badges') else 'false'
        
        conn.execute('UPDATE settings SET value = ? WHERE key = "dark_mode"', (dark_mode,))
        conn.execute('UPDATE settings SET value = ? WHERE key = "show_tip_badges"', (show_tip_badges,))
        
        conn.commit()
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'status': 'success'})
        return redirect(url_for('index'))
    
    # Check and update fiscal year if needed
    update_fiscal_year_if_needed()
    
    # Get all settings for the template
    all_associates = get_all_associates()
    active_associates = get_active_associates()
    hidden_associates = get_hidden_associates()
    year_type = get_year_type()
    
    # Get fiscal dates
    fiscal_start = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_start"').fetchone()['value']
    fiscal_end = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_end"').fetchone()['value']
    
    # Get timezone settings
    current_timezone = get_current_timezone(conn)
    timezones_by_region = get_timezones_by_region()
    
    # Get display settings
    dark_mode = conn.execute('SELECT value FROM settings WHERE key = "dark_mode"').fetchone()['value']
    show_tip_badges = conn.execute('SELECT value FROM settings WHERE key = "show_tip_badges"').fetchone()['value']
    
    # Get last update time
    last_update = conn.execute('SELECT value FROM settings WHERE key = "last_order_update"').fetchone()
    if last_update and last_update['value'] != 'Never':
        date_obj = datetime.strptime(last_update['value'], '%Y-%m-%d')
        last_update_time = date_obj.strftime('%m/%d/%Y')
    else:
        last_update_time = 'Never'
    
    # Get detailed stats for all associates
    detailed_stats = {}
    for associate in all_associates:
        stats = conn.execute('''
            WITH daily_stats AS (
                SELECT 
                    score_date,
                    sales_associate,
                    daily_score,
                    RANK() OVER (PARTITION BY score_date ORDER BY daily_score DESC) as rank_for_day
                FROM somm_scores 
                GROUP BY score_date, sales_associate
            ),
            team_avg AS (
                SELECT COALESCE(ROUND(AVG(daily_score), 2), 0) as team_average
                FROM somm_scores
                WHERE score_date >= date('now', '-30 days')
            )
            SELECT 
                COALESCE(COUNT(*), 0) as total_days,
                COALESCE(ROUND(AVG(daily_score), 2), 0) as avg_score,
                COALESCE(ROUND(AVG(CASE WHEN rank_for_day = 1 THEN 1 ELSE 0 END) * 100, 1), 0) as top_performer_pct,
                COALESCE(COUNT(CASE WHEN daily_score >= 75 THEN 1 END), 0) as days_above_75,
                COALESCE((SELECT team_average FROM team_avg), 0) as team_avg,
                COALESCE(ROUND(AVG(daily_score) - (SELECT team_average FROM team_avg), 1), 0) as diff_from_avg
            FROM daily_stats
            WHERE sales_associate = ?
        ''', (associate,)).fetchone()
        
        if stats:
            detailed_stats[associate] = dict(stats)
        else:
            detailed_stats[associate] = {
                'total_days': 0,
                'avg_score': 0,
                'top_performer_pct': 0,
                'days_above_75': 0,
                'team_avg': 0,
                'diff_from_avg': 0
            }
    
    conn.close()
    
    return render_template('settings.html',
                         all_associates=all_associates,
                         active_associates=active_associates,
                         hidden_associates=hidden_associates,
                         year_type=year_type,
                         fiscal_start=fiscal_start,
                         fiscal_end=fiscal_end,
                         dark_mode=dark_mode,
                         show_tip_badges=show_tip_badges,
                         last_update_time=last_update_time,
                         detailed_stats=detailed_stats,
                         current_timezone=current_timezone,
                         timezones_by_region=timezones_by_region)

@app.route('/setup/progress')
def setup_progress_status():
    """Return the current setup progress"""
    return jsonify(setup_progress)

@app.route('/setup', methods=['GET', 'POST'])
def setup_wizard():
    if is_initialized():
        return redirect(url_for('index'))
    
    # Set default values for year type and timezone
    year_type = 'calendar'
    timezone = 'America/Los_Angeles'
    current_year = datetime.now().year
    start_date = f"{current_year}-01-01"
    
    # Reset progress tracking
    global setup_progress
    setup_progress = {
        'status': 'starting',
        'message': 'Starting setup process...',
        'error': None
    }
    
    # Start setup process in background thread
    setup_thread = threading.Thread(
        target=process_setup,
        args=(year_type, start_date, timezone, setup_progress)
    )
    setup_thread.start()
    
    return redirect(url_for('team_setup'))

@app.route('/team_setup', methods=['GET', 'POST'])
def team_setup():
    """Setup page for selecting active associates after initial build."""
    if request.method == 'POST':
        active_associates = json.loads(request.form.get('active_associates', '[]'))
        hidden_associates = json.loads(request.form.get('hidden_associates', '[]'))
        
        conn = get_db_connection()
        conn.execute('UPDATE settings SET value = ? WHERE key = "active_associates"', 
                    (json.dumps(active_associates),))
        conn.execute('UPDATE settings SET value = ? WHERE key = "hidden_associates"', 
                    (json.dumps(hidden_associates),))
        conn.commit()
        conn.close()
        
        return redirect(url_for('index'))
    
    conn = get_db_connection()
    dark_mode = conn.execute('SELECT value FROM settings WHERE key = "dark_mode"').fetchone()['value']
    
    # Get all associates
    all_associates = get_all_associates()
    active_associates = get_active_associates()
    hidden_associates = get_hidden_associates()
    
    # Get associate stats
    associate_stats = {}
    for associate in all_associates:
        stats = conn.execute('''
            WITH daily_stats AS (
                SELECT 
                    score_date,
                    sales_associate,
                    daily_score,
                    RANK() OVER (PARTITION BY score_date ORDER BY daily_score DESC) as rank_for_day
                FROM somm_scores 
                GROUP BY score_date, sales_associate
            ),
            team_avg AS (
                SELECT ROUND(AVG(daily_score), 2) as team_average
                FROM somm_scores
                WHERE score_date >= date('now', '-30 days')
            )
            SELECT 
                COALESCE(COUNT(*), 0) as days_counted,
                COALESCE(ROUND(AVG(daily_score), 2), 0) as average_score,
                COALESCE(ROUND(AVG(daily_score) - (SELECT team_average FROM team_avg), 1), 0) as diff_from_avg
            FROM daily_stats
            WHERE sales_associate = ?
        ''', (associate,)).fetchone()
        
        if stats:
            associate_stats[associate] = {
                'days_counted': stats['days_counted'],
                'average_score': stats['average_score'],
                'diff_from_avg': stats['diff_from_avg']
            }
        else:
            associate_stats[associate] = {
                'days_counted': 0,
                'average_score': 0,
                'diff_from_avg': 0
            }
    
    conn.close()
    
    return render_template('team_setup.html', 
                         all_associates=all_associates,
                         active_associates=active_associates,
                         hidden_associates=hidden_associates,
                         dark_mode=dark_mode,
                         associate_stats=associate_stats)

@app.route('/')
def index():
    if not initialization_complete:
        return render_template('initializing.html'), 503
    # Check if active associates are set up
    active_associates = get_active_associates()
    if not active_associates:
        return redirect(url_for('team_setup'))
        
    # Check and update fiscal year if needed
    update_fiscal_year_if_needed()
    
    colors = [
        '#2563eb', '#16a34a', '#dc2626', '#9333ea', '#ea580c', 
        '#0891b2', '#4f46e5', '#be123c', '#854d0e', '#115e59', 
        '#701a75', '#1e293b'
    ]
    
    conn = get_db_connection()
    
    # Get dark mode setting first
    dark_mode = conn.execute('SELECT value FROM settings WHERE key = "dark_mode"').fetchone()['value']
    show_tip_badges = conn.execute('SELECT value FROM settings WHERE key = "show_tip_badges"').fetchone()['value']
    
    # Get year type and fiscal dates
    year_type = conn.execute('SELECT value FROM settings WHERE key = "year_type"').fetchone()['value']
    fiscal_start = None
    fiscal_end = None
    
    if year_type == 'fiscal':
        fiscal_start = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_start"').fetchone()['value']
        fiscal_end = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_end"').fetchone()['value']
    
    # Calculate start date based on year type
    today = datetime.now()
    if year_type == 'fiscal' and fiscal_start:
        # Parse fiscal year dates
        fiscal_start_date = datetime.strptime(fiscal_start, '%Y-%m-%d')
        
        # Calculate current fiscal year
        if today >= fiscal_start_date.replace(year=today.year):
            current_fiscal_year = today.year
        else:
            current_fiscal_year = today.year - 1
            
        # Set start date to beginning of current fiscal year
        start_date = fiscal_start_date.replace(year=current_fiscal_year)
    else:
        # Calendar year - start from January 1st
        start_date = today.replace(month=1, day=1)
    
    # Calculate team average SommScore for year-to-date
    team_metrics = conn.execute('''
        WITH recent_scores AS (
            SELECT 
                score_date,
                ROUND(AVG(daily_score), 2) as team_daily_score
            FROM somm_scores
            WHERE sales_associate IN ({})
            AND score_date >= ?
            GROUP BY score_date
        )
        SELECT 
            ROUND(AVG(team_daily_score), 2) as team_avg_score,
            COUNT(DISTINCT score_date) as days_counted
        FROM recent_scores
    '''.format(','.join(['?'] * len(active_associates))), 
    [*active_associates, start_date.strftime('%Y-%m-%d')]).fetchone()
    
    # Calculate team revenue metrics
    revenue_metrics = conn.execute('''
        WITH current_period AS (
            SELECT 
                ROUND(AVG(daily_total), 2) as avg_daily_revenue,
                COUNT(DISTINCT order_date) as days_counted
            FROM (
                SELECT 
                    date(order_paid_date) as order_date,
                    SUM(subtotal) as daily_total
                FROM orders
                WHERE date(order_paid_date) >= ?
                GROUP BY date(order_paid_date)
            )
        ),
        ref_period AS (
            SELECT 
                ROUND(AVG(daily_total), 2) as ref_daily_revenue
            FROM (
                SELECT 
                    date(order_paid_date) as order_date,
                    SUM(subtotal) as daily_total
                FROM orders
                WHERE date(order_paid_date) >= date(?, '-365 days')
                AND date(order_paid_date) < ?
                GROUP BY date(order_paid_date)
            )
        )
        SELECT 
            cp.avg_daily_revenue,
            rp.ref_daily_revenue,
            ROUND(((cp.avg_daily_revenue - rp.ref_daily_revenue) / rp.ref_daily_revenue) * 100, 1) as revenue_performance
        FROM current_period cp, ref_period rp
    ''', (start_date.strftime('%Y-%m-%d'), 
          start_date.strftime('%Y-%m-%d'), 
          start_date.strftime('%Y-%m-%d'))).fetchone()
    
    # Calculate team club signup metrics
    club_metrics = conn.execute('''
        WITH current_period AS (
            SELECT 
                ROUND(COUNT(*) * 1.0 / CAST(COUNT(DISTINCT date(club_signup_date)) AS FLOAT), 2) as avg_daily_clubs
            FROM clubs
            WHERE date(club_signup_date) >= ?
        ),
        ref_period AS (
            SELECT 
                ROUND(COUNT(*) * 1.0 / CAST(COUNT(DISTINCT date(club_signup_date)) AS FLOAT), 2) as ref_daily_clubs
            FROM clubs
            WHERE date(club_signup_date) >= date(?, '-365 days')
            AND date(club_signup_date) < ?
        )
        SELECT 
            cp.avg_daily_clubs,
            rp.ref_daily_clubs,
            ROUND(((cp.avg_daily_clubs - rp.ref_daily_clubs) / rp.ref_daily_clubs) * 100, 1) as club_performance
        FROM current_period cp, ref_period rp
    ''', (start_date.strftime('%Y-%m-%d'), 
          start_date.strftime('%Y-%m-%d'), 
          start_date.strftime('%Y-%m-%d'))).fetchone()
    
    # Calculate overall team grade
    # Weight factors: SommScore (40%), Revenue (40%), Club Signups (20%)
    somm_score_weight = 0.4
    revenue_weight = 0.4
    club_weight = 0.2
    
    somm_score_grade = min(100, max(0, team_metrics['team_avg_score']))
    revenue_grade = min(100, max(0, 50 + revenue_metrics['revenue_performance']))
    club_grade = min(100, max(0, 50 + club_metrics['club_performance']))
    
    team_grade = round(
        somm_score_grade * somm_score_weight +
        revenue_grade * revenue_weight +
        club_grade * club_weight,
        1
    )
    
    team_performance = {
        'somm_score': {
            'value': team_metrics['team_avg_score'],
            'grade': somm_score_grade
        },
        'revenue': {
            'current': revenue_metrics['avg_daily_revenue'],
            'reference': revenue_metrics['ref_daily_revenue'],
            'performance': revenue_metrics['revenue_performance'],
            'grade': revenue_grade
        },
        'clubs': {
            'current': club_metrics['avg_daily_clubs'],
            'reference': club_metrics['ref_daily_clubs'],
            'performance': club_metrics['club_performance'],
            'grade': club_grade
        },
        'overall_grade': team_grade
    }
    
    # Get overall rankings
    rankings = conn.execute('''
        WITH avg_scores AS (
            SELECT 
                sales_associate,
                COUNT(*) as days_counted,
                ROUND(AVG(daily_score), 2) as average_score,
                ROUND(MIN(daily_score), 2) as min_score,
                ROUND(MAX(daily_score), 2) as max_score
            FROM somm_scores
            WHERE sales_associate IN ({})
            AND score_date >= ?
            GROUP BY sales_associate
        ),
        team_avg AS (
            SELECT 
                ROUND(AVG(team_daily_score), 2) as team_average
            FROM (
                SELECT 
                    score_date,
                    ROUND(AVG(daily_score), 2) as team_daily_score
                FROM somm_scores
                WHERE sales_associate IN ({})
                AND score_date >= ?
                GROUP BY score_date
            )
        )
        SELECT 
            avg_scores.*,
            team_avg.team_average,
            ROUND(avg_scores.average_score - team_avg.team_average, 1) as diff_from_avg
        FROM avg_scores, team_avg
        ORDER BY average_score DESC
    '''.format(','.join(['?'] * len(active_associates)), 
               ','.join(['?'] * len(active_associates))), 
    [*active_associates, start_date.strftime('%Y-%m-%d'), 
     *active_associates, start_date.strftime('%Y-%m-%d')]).fetchall()
    
    # Modify to 14 days and ensure we get all dates
    fourteen_days_ago = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    trends = conn.execute('''
        SELECT 
            score_date,
            sales_associate,
            daily_score
        FROM somm_scores
        WHERE score_date >= ? 
        AND sales_associate IN ({})
        ORDER BY score_date ASC
    '''.format(','.join(['?'] * len(active_associates))), 
    [fourteen_days_ago] + list(active_associates)).fetchall()
    
    # Process trend data with date filling
    trend_data = {associate: [] for associate in active_associates}
    
    # Get all dates in range
    start_date = datetime.strptime(fourteen_days_ago, '%Y-%m-%d')
    dates = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
            for x in range(15)]  # 15 to include both start and end date
    
    # Initialize all dates with null scores
    for associate in active_associates:
        for date in dates:
            trend_data[associate].append({
                'date': date,
                'score': None
            })
    
    # Fill in actual scores where we have them
    for row in trends:
        if row['sales_associate'] in trend_data:
            date_index = dates.index(row['score_date'])
            trend_data[row['sales_associate']][date_index]['score'] = row['daily_score']
    
    # Add detailed stats query
    detailed_stats = {}
    for associate in active_associates:
        stats = conn.execute('''
            WITH daily_stats AS (
                SELECT 
                    score_date,
                    sales_associate,
                    daily_score,
                    RANK() OVER (PARTITION BY score_date ORDER BY daily_score DESC) as rank_for_day
                FROM somm_scores 
                GROUP BY score_date, sales_associate
            ),
            active_associates AS (
                SELECT json_each.value as sales_associate
                FROM settings, json_each(settings.value)
                WHERE settings.key = 'active_associates'
            ),
            sales_totals AS (
                SELECT 
                    SUM(subtotal) as total_sales,
                    COUNT(*) as total_orders
                FROM orders
                JOIN active_associates ON orders.sales_associate = active_associates.sales_associate
                WHERE strftime('%Y', order_paid_date) = strftime('%Y', 'now')
            ),
            club_totals AS (
                SELECT COUNT(*) as total_clubs
                FROM clubs
                JOIN active_associates ON clubs.sales_associate = active_associates.sales_associate
                WHERE strftime('%Y', club_signup_date) = strftime('%Y', 'now')
            ),
            last_seven_days AS (
                SELECT ROUND(AVG(daily_score), 2) as seven_day_avg
                FROM somm_scores
                WHERE sales_associate = ?
                AND score_date >= date('now', '-7 days')
            ),
            month_to_date AS (
                SELECT ROUND(AVG(daily_score), 2) as mtd_avg
                FROM somm_scores
                WHERE sales_associate = ?
                AND strftime('%Y-%m', score_date) = strftime('%Y-%m', 'now')
            )
            SELECT 
                COUNT(*) as total_days,
                ROUND(AVG(daily_score), 2) as avg_score,
                ROUND(AVG(CASE WHEN rank_for_day = 1 THEN 1 ELSE 0 END) * 100, 1) as top_performer_pct,
                COUNT(CASE WHEN daily_score >= 75 THEN 1 END) as days_above_75,
                ROUND((SELECT COUNT(*) FROM clubs WHERE sales_associate = ? AND strftime('%Y', club_signup_date) = strftime('%Y', 'now')) * 100.0 / (SELECT total_clubs FROM club_totals), 1) as club_signup_pct,
                ROUND((SELECT SUM(subtotal) FROM orders WHERE sales_associate = ? AND strftime('%Y', order_paid_date) = strftime('%Y', 'now')) * 100.0 / (SELECT total_sales FROM sales_totals), 1) as sales_dollars_pct,
                (SELECT seven_day_avg FROM last_seven_days) as seven_day_avg,
                (SELECT mtd_avg FROM month_to_date) as mtd_avg
            FROM daily_stats
            WHERE sales_associate = ?
            AND EXISTS (
                SELECT 1 FROM active_associates aa 
                WHERE daily_stats.sales_associate = aa.sales_associate
            )
        ''', (associate, associate, associate, associate, associate)).fetchone()
        
        detailed_stats[associate] = dict(stats)
    
    # Get tip leaders
    tip_leaders = conn.execute('''
        WITH active_associates AS (
            SELECT json_each.value as sales_associate
            FROM settings, json_each(settings.value)
            WHERE settings.key = 'active_associates'
        ),
        weekly_tips AS (
            SELECT 
                orders.sales_associate,
                ROUND(SUM(tip_total), 2) as tip_sum,
                'weekly' as period,
                ROUND(SUM(tip_total) * 100.0 / (
                    SELECT SUM(tip_total) 
                    FROM orders o2 
                    JOIN active_associates aa2 ON o2.sales_associate = aa2.sales_associate
                    WHERE order_paid_date >= date('now', '-7 days')
                ), 1) as tip_share
            FROM orders 
            JOIN active_associates ON orders.sales_associate = active_associates.sales_associate
            WHERE order_paid_date >= date('now', '-7 days')
            GROUP BY orders.sales_associate
            ORDER BY tip_share DESC
            LIMIT 1
        ),
        monthly_tips AS (
            SELECT 
                orders.sales_associate,
                ROUND(SUM(tip_total), 2) as tip_sum,
                'monthly' as period,
                ROUND(SUM(tip_total) * 100.0 / (
                    SELECT SUM(tip_total) 
                    FROM orders o2 
                    JOIN active_associates aa2 ON o2.sales_associate = aa2.sales_associate
                    WHERE date(order_paid_date) >= date('now', 'start of month')
                ), 1) as tip_share
            FROM orders 
            JOIN active_associates ON orders.sales_associate = active_associates.sales_associate
            WHERE date(order_paid_date) >= date('now', 'start of month')
            GROUP BY orders.sales_associate
            ORDER BY tip_share DESC
            LIMIT 1
        ),
        yearly_tips AS (
            SELECT 
                orders.sales_associate,
                ROUND(SUM(tip_total), 2) as tip_sum,
                'yearly' as period,
                ROUND(SUM(tip_total) * 100.0 / (
                    SELECT SUM(tip_total) 
                    FROM orders o2 
                    JOIN active_associates aa2 ON o2.sales_associate = aa2.sales_associate
                    WHERE strftime('%Y', order_paid_date) = strftime('%Y', 'now')
                ), 1) as tip_share
            FROM orders 
            JOIN active_associates ON orders.sales_associate = active_associates.sales_associate
            WHERE strftime('%Y', order_paid_date) = strftime('%Y', 'now')
            GROUP BY orders.sales_associate
            ORDER BY tip_share DESC
            LIMIT 1
        )
        SELECT * FROM weekly_tips
        UNION ALL
        SELECT * FROM monthly_tips
        UNION ALL
        SELECT * FROM yearly_tips
    ''').fetchall()
    
    # Get revenue leaders
    revenue_leaders = conn.execute('''
        WITH active_associates AS (
            SELECT json_each.value as sales_associate
            FROM settings, json_each(settings.value)
            WHERE settings.key = 'active_associates'
        ),
        weekly_revenue AS (
            SELECT 
                orders.sales_associate,
                ROUND(SUM(subtotal), 2) as revenue_sum,
                'weekly' as period,
                ROUND(SUM(subtotal) * 100.0 / (
                    SELECT SUM(subtotal) 
                    FROM orders o2 
                    JOIN active_associates aa2 ON o2.sales_associate = aa2.sales_associate
                    WHERE order_paid_date >= date('now', '-7 days')
                ), 1) as revenue_share
            FROM orders 
            JOIN active_associates ON orders.sales_associate = active_associates.sales_associate
            WHERE order_paid_date >= date('now', '-7 days')
            AND orders.sales_associate IS NOT NULL
            GROUP BY orders.sales_associate
            ORDER BY revenue_share DESC
            LIMIT 1
        ),
        monthly_revenue AS (
            SELECT 
                orders.sales_associate,
                ROUND(SUM(subtotal), 2) as revenue_sum,
                'monthly' as period,
                ROUND(SUM(subtotal) * 100.0 / (
                    SELECT SUM(subtotal) 
                    FROM orders o2 
                    JOIN active_associates aa2 ON o2.sales_associate = aa2.sales_associate
                    WHERE date(order_paid_date) >= date('now', 'start of month')
                ), 1) as revenue_share
            FROM orders 
            JOIN active_associates ON orders.sales_associate = active_associates.sales_associate
            WHERE date(order_paid_date) >= date('now', 'start of month')
            AND orders.sales_associate IS NOT NULL
            GROUP BY orders.sales_associate
            ORDER BY revenue_share DESC
            LIMIT 1
        ),
        yearly_revenue AS (
            SELECT 
                orders.sales_associate,
                ROUND(SUM(subtotal), 2) as revenue_sum,
                'yearly' as period,
                ROUND(SUM(subtotal) * 100.0 / (
                    SELECT SUM(subtotal) 
                    FROM orders o2 
                    JOIN active_associates aa2 ON o2.sales_associate = aa2.sales_associate
                    WHERE strftime('%Y', order_paid_date) = strftime('%Y', 'now')
                ), 1) as revenue_share
            FROM orders 
            JOIN active_associates ON orders.sales_associate = active_associates.sales_associate
            WHERE strftime('%Y', order_paid_date) = strftime('%Y', 'now')
            AND orders.sales_associate IS NOT NULL
            GROUP BY orders.sales_associate
            ORDER BY revenue_share DESC
            LIMIT 1
        )
        SELECT * FROM weekly_revenue
        UNION ALL
        SELECT * FROM monthly_revenue
        UNION ALL
        SELECT * FROM yearly_revenue
    ''').fetchall()
    
    # Get club signup leaders
    club_leaders = conn.execute('''
        WITH active_associates AS (
            SELECT json_each.value as sales_associate
            FROM settings, json_each(settings.value)
            WHERE settings.key = 'active_associates'
        ),
        weekly_clubs AS (
            SELECT 
                clubs.sales_associate,
                COUNT(*) as club_count,
                'weekly' as period,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(*) 
                    FROM clubs c2 
                    JOIN active_associates aa2 ON c2.sales_associate = aa2.sales_associate
                    WHERE club_signup_date >= date('now', '-7 days')
                ), 1) as club_share
            FROM clubs 
            JOIN active_associates ON clubs.sales_associate = active_associates.sales_associate
            WHERE club_signup_date >= date('now', '-7 days')
            GROUP BY clubs.sales_associate
            ORDER BY club_share DESC
            LIMIT 1
        ),
        monthly_clubs AS (
            SELECT 
                clubs.sales_associate,
                COUNT(*) as club_count,
                'monthly' as period,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(*) 
                    FROM clubs c2 
                    JOIN active_associates aa2 ON c2.sales_associate = aa2.sales_associate
                    WHERE date(club_signup_date) >= date('now', 'start of month')
                ), 1) as club_share
            FROM clubs 
            JOIN active_associates ON clubs.sales_associate = active_associates.sales_associate
            WHERE date(club_signup_date) >= date('now', 'start of month')
            GROUP BY clubs.sales_associate
            ORDER BY club_share DESC
            LIMIT 1
        ),
        yearly_clubs AS (
            SELECT 
                clubs.sales_associate,
                COUNT(*) as club_count,
                'yearly' as period,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(*) 
                    FROM clubs c2 
                    JOIN active_associates aa2 ON c2.sales_associate = aa2.sales_associate
                    WHERE strftime('%Y', club_signup_date) = strftime('%Y', 'now')
                ), 1) as club_share
            FROM clubs 
            JOIN active_associates ON clubs.sales_associate = active_associates.sales_associate
            WHERE strftime('%Y', club_signup_date) = strftime('%Y', 'now')
            GROUP BY clubs.sales_associate
            ORDER BY club_share DESC
            LIMIT 1
        )
        SELECT * FROM weekly_clubs
        UNION ALL
        SELECT * FROM monthly_clubs
        UNION ALL
        SELECT * FROM yearly_clubs
    ''').fetchall()
    
    # Convert to dictionaries for easy access in template
    tip_leaders_dict = {
        'weekly': {'name': None, 'amount': 0},
        'monthly': {'name': None, 'amount': 0},
        'yearly': {'name': None, 'amount': 0}
    }
    
    revenue_leaders_dict = {
        'weekly': {'name': None, 'amount': 0},
        'monthly': {'name': None, 'amount': 0},
        'yearly': {'name': None, 'amount': 0}
    }
    
    club_leaders_dict = {
        'weekly': {'name': None, 'amount': 0},
        'monthly': {'name': None, 'amount': 0},
        'yearly': {'name': None, 'amount': 0}
    }
    
    # Update with actual values from database
    for row in tip_leaders:
        if row['period'] in tip_leaders_dict:
            tip_leaders_dict[row['period']] = {
                'name': row['sales_associate'],
                'amount': row['tip_sum']
            }
            
    for row in revenue_leaders:
        if row['period'] in revenue_leaders_dict:
            revenue_leaders_dict[row['period']] = {
                'name': row['sales_associate'],
                'amount': row['revenue_sum']
            }
            
    for row in club_leaders:
        if row['period'] in club_leaders_dict:
            club_leaders_dict[row['period']] = {
                'name': row['sales_associate'],
                'amount': row['club_count']
            }
    
    # Add debug logging after we have all the data
    print("\nFinal Leaders Data:")
    print("Tips:", tip_leaders_dict)
    print("Revenue:", revenue_leaders_dict)
    print("Clubs:", club_leaders_dict)
    print("\nShow Tip Badges Setting:", show_tip_badges)
    
    # Close connection at the end
    conn.close()
    
    return render_template('index.html', 
                         rankings=rankings,
                         trend_data=json.dumps(trend_data),
                         dates=json.dumps(dates),
                         colors=colors,
                         detailed_stats=json.dumps(detailed_stats),
                         dark_mode=dark_mode,
                         tip_leaders=tip_leaders_dict,
                         revenue_leaders=revenue_leaders_dict,
                         club_leaders=club_leaders_dict,
                         show_tip_badges=show_tip_badges,
                         team_performance=team_performance)

@app.route('/help')
def help():
    conn = get_db_connection()
    dark_mode = conn.execute('SELECT value FROM settings WHERE key = "dark_mode"').fetchone()['value']
    conn.close()
    return render_template('help.html', dark_mode=dark_mode)

@app.route('/trends')
def trends():
    if not initialization_complete:
        return render_template('initializing.html'), 503
    active_associates = get_active_associates()
    if not active_associates:
        return redirect(url_for('settings'))
    
    colors = [
        '#2563eb', '#16a34a', '#dc2626', '#9333ea', '#ea580c', 
        '#0891b2', '#4f46e5', '#be123c', '#854d0e', '#115e59', 
        '#701a75', '#1e293b'
    ]
    
    conn = get_db_connection()
    dark_mode = conn.execute('SELECT value FROM settings WHERE key = "dark_mode"').fetchone()['value']
    
    fourteen_days_ago = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    trends = conn.execute('''
        SELECT score_date, sales_associate, daily_score
        FROM somm_scores
        WHERE score_date >= ? 
        AND sales_associate IN ({})
        ORDER BY score_date ASC
    '''.format(','.join(['?'] * len(active_associates))), 
    [fourteen_days_ago] + list(active_associates)).fetchall()
    
    trend_data = {associate: [] for associate in active_associates}
    start_date = datetime.strptime(fourteen_days_ago, '%Y-%m-%d')
    dates = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
            for x in range(15)]
    
    for associate in active_associates:
        for date in dates:
            trend_data[associate].append({
                'date': date,
                'score': None
            })
    
    for row in trends:
        if row['sales_associate'] in trend_data:
            date_index = dates.index(row['score_date'])
            trend_data[row['sales_associate']][date_index]['score'] = row['daily_score']
    
    conn.close()
    
    return render_template('trends.html',
                         active_associates=active_associates,
                         trend_data=json.dumps(trend_data),
                         dates=json.dumps(dates),
                         colors=colors,
                         dark_mode=dark_mode)

@app.route('/api/trends')
def get_trend_data():
    duration = request.args.get('duration', '14')
    metric_type = request.args.get('metric_type', 'daily')
    active_associates = get_active_associates()
    
    if not active_associates:
        return jsonify({'error': 'No active associates'}), 400

    conn = get_db_connection()
    
    # Get year type and fiscal dates if needed
    year_type = conn.execute('SELECT value FROM settings WHERE key = "year_type"').fetchone()['value']
    fiscal_start = None
    fiscal_end = None
    
    if year_type == 'fiscal':
        fiscal_start = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_start"').fetchone()['value']
        fiscal_end = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_end"').fetchone()['value']
    
    # Calculate the start date based on duration
    today = datetime.now()
    
    if duration == '7':
        display_start_date = today - timedelta(days=7)
    elif duration == '14':
        display_start_date = today - timedelta(days=14)
    elif duration == 'mtd':
        display_start_date = today.replace(day=1)
    elif duration == 'qtd' or duration == 'ytd':
        if year_type == 'fiscal' and fiscal_start and fiscal_end:
            # Parse fiscal year dates
            fiscal_start_date = datetime.strptime(fiscal_start, '%Y-%m-%d')
            
            # Calculate current fiscal year
            if today >= fiscal_start_date.replace(year=today.year):
                current_fiscal_year = today.year
            else:
                current_fiscal_year = today.year - 1
            
            if duration == 'ytd':
                # Start from the beginning of fiscal year
                display_start_date = fiscal_start_date.replace(year=current_fiscal_year)
            else:  # qtd
                # Calculate which fiscal quarter we're in
                fiscal_year_start = fiscal_start_date.replace(year=current_fiscal_year)
                months_since_fiscal_start = (today.year - fiscal_year_start.year) * 12 + today.month - fiscal_year_start.month
                current_fiscal_quarter = (months_since_fiscal_start // 3)
                
                # Calculate the start of the current fiscal quarter
                quarter_start_month = fiscal_start_date.month + (current_fiscal_quarter * 3)
                if quarter_start_month > 12:
                    quarter_start_month -= 12
                    current_fiscal_year += 1
                
                display_start_date = datetime(current_fiscal_year, quarter_start_month, 1)
        else:
            # Calendar year calculations
            if duration == 'ytd':
                display_start_date = today.replace(month=1, day=1)
            else:  # qtd
                quarter = (today.month - 1) // 3
                display_start_date = today.replace(month=quarter * 3 + 1, day=1)
    else:
        display_start_date = today - timedelta(days=14)  # Default to 14 days

    display_start_date_str = display_start_date.strftime('%Y-%m-%d')
    
    # Calculate the year start date for cumulative calculations
    if year_type == 'fiscal' and fiscal_start:
        fiscal_start_date = datetime.strptime(fiscal_start, '%Y-%m-%d')
        if today >= fiscal_start_date.replace(year=today.year):
            year_start_date = fiscal_start_date.replace(year=today.year)
        else:
            year_start_date = fiscal_start_date.replace(year=today.year - 1)
    else:
        year_start_date = today.replace(month=1, day=1)
    
    year_start_date_str = year_start_date.strftime('%Y-%m-%d')
    
    # Get all scores from year start for cumulative calculations
    year_scores = conn.execute('''
        SELECT score_date, sales_associate, daily_score
        FROM somm_scores
        WHERE score_date >= ? 
        AND sales_associate IN ({})
        ORDER BY score_date ASC, sales_associate
    '''.format(','.join(['?'] * len(active_associates))), 
    [year_start_date_str] + list(active_associates)).fetchall()
    
    # Get trend data for the selected display period
    trends = conn.execute('''
        SELECT score_date, sales_associate, daily_score
        FROM somm_scores
        WHERE score_date >= ? 
        AND sales_associate IN ({})
        ORDER BY score_date ASC, sales_associate
    '''.format(','.join(['?'] * len(active_associates))), 
    [display_start_date_str] + list(active_associates)).fetchall()
    
    # Process trend data
    trend_data = {associate: [] for associate in active_associates}
    dates = []
    current_date = display_start_date
    
    while current_date <= today:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    # Initialize data structure with null scores
    for associate in active_associates:
        for date in dates:
            trend_data[associate].append({
                'date': date,
                'score': None,
                'cumulative': None
            })
    
    # Create lookup for year-to-date scores
    ytd_scores = {associate: [] for associate in active_associates}
    for row in year_scores:
        associate = row['sales_associate']
        if associate in ytd_scores:
            ytd_scores[associate].append({
                'date': row['score_date'],
                'score': row['daily_score']
            })
    
    # Fill in actual scores and calculate cumulative averages
    for row in trends:
        if row['sales_associate'] in trend_data:
            date_index = dates.index(row['score_date'])
            associate = row['sales_associate']
            
            # Update daily score
            trend_data[associate][date_index]['score'] = row['daily_score']
            
            # Calculate cumulative average using all scores up to this date
            if associate in ytd_scores:
                scores_up_to_date = [s['score'] for s in ytd_scores[associate] 
                                   if s['date'] <= row['score_date'] and s['score'] is not None]
                if scores_up_to_date:
                    trend_data[associate][date_index]['cumulative'] = round(sum(scores_up_to_date) / len(scores_up_to_date), 2)
    
    conn.close()
    
    return jsonify({
        'dates': dates,
        'trends': trend_data
    })

def recalculate_scores():
    """Recalculate SommScores for all data in the current period."""
    try:
        logger.info("Starting SommScore recalculation...")
        
        conn = get_db_connection()
        
        # Get year type and start date
        year_type = conn.execute('SELECT value FROM settings WHERE key = "year_type"').fetchone()['value']
        
        # Determine start date based on year type
        today = datetime.now()
        if year_type == 'fiscal':
            fiscal_start = conn.execute('SELECT value FROM settings WHERE key = "fiscal_year_start"').fetchone()['value']
            fiscal_start_date = datetime.strptime(fiscal_start, '%Y-%m-%d')
            
            # Calculate current fiscal year
            if today >= fiscal_start_date.replace(year=today.year):
                current_fiscal_year = today.year
            else:
                current_fiscal_year = today.year - 1
                
            start_date = fiscal_start_date.replace(year=current_fiscal_year)
        else:
            # Calendar year - start from January 1st
            start_date = today.replace(month=1, day=1)
        
        # Import and run score calculation
        from calc_somm_score import calculate_somm_scores
        calculate_somm_scores(None, conn, start_date.strftime('%Y-%m-%d'))
        
        logger.info("SommScore recalculation completed successfully")
        
    except Exception as e:
        logger.error(f"Error during SommScore recalculation: {e}")
    finally:
        if conn:
            conn.close()

@app.route('/manual_update', methods=['POST'])
def manual_update():
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        if not start_date:
            return jsonify({'error': 'Start date is required'}), 400
            
        # Get today's date as end date
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Run update for the date range
        from daily_update import update_data
        update_data(start_date=start_date, end_date=end_date)
        
        # Get updated list of associates
        all_associates = get_all_associates()
        active_associates = get_active_associates()
        hidden_associates = get_hidden_associates()
        
        # Return success message with updated associate lists
        return jsonify({
            'message': 'Update completed successfully. Check the logs for details.',
            'all_associates': all_associates,
            'active_associates': active_associates,
            'hidden_associates': hidden_associates
        })
            
    except Exception as e:
        app.logger.error(f"Error during manual update: {str(e)}")
        return jsonify({'error': str(e)}), 500

def wait_for_tables():
    """Wait for all required tables to be created and populated."""
    max_attempts = 10
    attempt = 0
    while attempt < max_attempts:
        try:
            conn = get_db_connection()
            # Check for all required tables
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name IN ('settings', 'orders', 'clubs', 'somm_scores', 'ref_table')
            """).fetchall()
            conn.close()
            
            # Convert to set of table names for easier comparison
            table_names = {table['name'] for table in tables}
            required_tables = {'settings', 'orders', 'clubs', 'somm_scores', 'ref_table'}
            
            if table_names == required_tables:  # All required tables exist
                return True
                
            print(f"Waiting for tables to be created... (attempt {attempt + 1}/{max_attempts})")
            print(f"Found tables: {table_names}")
            print(f"Missing tables: {required_tables - table_names}")
            time.sleep(2)  # Wait 2 seconds before next attempt
            attempt += 1
        except Exception as e:
            print(f"Error checking tables: {str(e)}")
            time.sleep(2)
            attempt += 1
    return False

def initialize_application():
    """Initialize the application with proper locking."""
    global initialization_complete
    
    with initialization_lock:
        if initialization_complete:
            return True
            
        try:
            # Initialize settings table and ensure all tables exist
            init_settings_table()
            
            # Ensure all required tables exist
            conn = get_db_connection()
            try:
                # Create tables if they don't exist
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        order_number TEXT PRIMARY KEY,
                        order_date TEXT,
                        order_paid_date TEXT,
                        sales_associate TEXT,
                        subtotal REAL,
                        tip_total REAL
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS clubs (
                        club_id TEXT PRIMARY KEY,
                        club_signup_date TEXT,
                        sales_associate TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS somm_scores (
                        score_date TEXT,
                        sales_associate TEXT,
                        daily_score REAL,
                        PRIMARY KEY (score_date, sales_associate)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS ref_table (
                        date TEXT PRIMARY KEY,
                        dow INTEGER,
                        mon INTEGER,
                        fisc_mon INTEGER,
                        ttl_earn REAL,
                        day_wght REAL
                    )
                """)
                conn.commit()
            finally:
                conn.close()
            
            if DEMO_MODE:
                print("Running in demo mode - generating fake data...")
                # Clear existing data in demo mode
                conn = get_db_connection()
                try:
                    conn.execute("DELETE FROM orders")
                    conn.execute("DELETE FROM clubs")
                    conn.execute("DELETE FROM somm_scores")
                    conn.execute("DELETE FROM ref_table")
                    conn.commit()
                finally:
                    conn.close()
                
                if not generate_fake_data():
                    print("Error: Failed to generate fake data. Exiting...")
                    return False
                print("Fake data generation complete!")
            elif is_initialized():
                recalculate_scores()
                init_scheduler()
            
            # Verify all tables exist and are accessible
            if not wait_for_tables():
                print("Error: Required tables were not created in time. Exiting...")
                return False
            
            initialization_complete = True
            return True
            
        except Exception as e:
            print(f"Error during initialization: {str(e)}")
            return False

# Initialize the application before starting the server
if not initialize_application():
    print("Failed to initialize application. Exiting...")
    exit(1)

@app.route('/api/status')
def check_status():
    """Check the initialization status of the application."""
    try:
        if not initialization_complete:
            # Check if tables exist
            conn = get_db_connection()
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name IN ('settings', 'orders', 'clubs', 'somm_scores', 'ref_table')
            """).fetchall()
            conn.close()
            
            # Convert to set of table names for easier comparison
            table_names = {table['name'] for table in tables}
            required_tables = {'settings', 'orders', 'clubs', 'somm_scores', 'ref_table'}
            
            if table_names != required_tables:
                return jsonify({
                    'initialized': False,
                    'error': f'Waiting for tables: {required_tables - table_names}'
                })
            
        return jsonify({
            'initialized': initialization_complete,
            'error': None
        })
    except Exception as e:
        return jsonify({
            'initialized': False,
            'error': f'Error checking status: {str(e)}'
        })

if __name__ == '__main__':
    app.run(debug=True) 