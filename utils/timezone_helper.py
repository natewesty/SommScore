import pytz
from datetime import datetime
import sqlite3
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

WINE_TIMEZONES = [
    ('America/Los_Angeles', 'Pacific Coast (Napa, Sonoma, Oregon)'),
    ('America/Vancouver', 'British Columbia Wine Region'),
    ('America/Denver', 'Colorado Wine Country'),
    ('America/Chicago', 'Texas Hill Country'),
    ('America/New_York', 'Finger Lakes, NY'),
    ('America/Santiago', 'Chilean Wine Region'),
    ('America/Argentina/Mendoza', 'Mendoza Wine Country'),
    ('Europe/Paris', 'French Wine Regions'),
    ('Europe/Madrid', 'Spanish Wine Country'),
    ('Europe/Rome', 'Italian Wine Regions'),
    ('Europe/Berlin', 'German Wine Regions'),
    ('Europe/Lisbon', 'Portuguese Wine Country'),
    ('Europe/Vienna', 'Austrian Wine Region'),
    ('Europe/Budapest', 'Hungarian Wine Region'),
    ('Australia/Adelaide', 'South Australian Wine Region'),
    ('Australia/Melbourne', 'Victorian Wine Region'),
    ('Australia/Sydney', 'New South Wales Wine Region'),
    ('Australia/Perth', 'Western Australian Wine Region'),
    ('Pacific/Auckland', 'New Zealand Wine Regions'),
    ('Asia/Tokyo', 'Japanese Wine Market'),
    ('Asia/Hong_Kong', 'Asian Wine Hub'),
    ('Africa/Johannesburg', 'South African Wine Region'),
    ('UTC', 'Coordinated Universal Time')
]

def validate_timezone(tz_name: str) -> bool:
    """
    Validate that a timezone name exists in the pytz database.
    
    Args:
        tz_name: The timezone name to validate
        
    Returns:
        bool: True if timezone is valid, False otherwise
    """
    try:
        pytz.timezone(tz_name)
        return True
    except pytz.exceptions.UnknownTimeZoneError:
        return False

def get_timezones_by_region() -> List[Tuple[str, str]]:
    """
    Get list of wine region timezones with UTC offsets.
    Returns a list of (timezone, display name with offset) tuples.
    """
    # Use naive datetime for timezone calculations
    now = datetime.utcnow()  # Use UTC as base time
    result = []
    
    for tz_name, display_name in WINE_TIMEZONES:
        try:
            # Verify timezone is valid before using it
            if not validate_timezone(tz_name):
                logger.warning(f"Invalid timezone {tz_name}, skipping")
                continue
                
            tz = pytz.timezone(tz_name)
            # Convert naive UTC datetime to this timezone
            local_dt = now.replace(tzinfo=pytz.UTC).astimezone(tz)
            offset = local_dt.strftime('%z')
            offset_str = f"UTC{offset[:3]}:{offset[3:]}"
            display = f"{display_name} ({offset_str})"
            result.append((tz_name, display))
        except Exception as e:
            logger.error(f"Error processing timezone {tz_name}: {str(e)}")
            continue
    
    return sorted(result, key=lambda x: x[1])  # Sort by display name

def get_current_timezone(db_path_or_conn) -> str:
    """
    Get the current timezone from settings.
    Args:
        db_path_or_conn: Either a database path string or an active connection
    Returns 'UTC' if no timezone is set.
    """
    try:
        should_close = False
        if isinstance(db_path_or_conn, str):
            conn = sqlite3.connect(db_path_or_conn)
            should_close = True
        else:
            conn = db_path_or_conn
            
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = 'timezone'")
        result = cursor.fetchone()
        
        if should_close:
            conn.close()
        
        return result[0] if result else 'UTC'
    except Exception as e:
        logger.error(f"Error getting timezone from settings: {str(e)}")
        return 'UTC'

def convert_to_utc(local_time: str, local_timezone: str) -> str:
    """
    Convert a local time string (HH:MM) to UTC time string based on timezone.
    Returns original time if conversion fails.
    """
    try:
        # Parse local time
        now = datetime.now()
        time_parts = local_time.split(':')
        local_dt = now.replace(hour=int(time_parts[0]), minute=int(time_parts[1]))
        
        # Convert to UTC
        local_tz = pytz.timezone(local_timezone)
        local_dt = local_tz.localize(local_dt)
        utc_dt = local_dt.astimezone(pytz.UTC)
        
        return utc_dt.strftime('%H:%M')
    except Exception as e:
        logger.error(f"Error converting time to UTC: {str(e)}")
        return local_time 