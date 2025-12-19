#!/usr/bin/env python3
"""Debug script to reproduce standby cancellation issue"""

from database import get_conn
from app_utils import get_daily_segments_data
from logic import get_shabbat_times_cache
from config import config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def debug_standby_cancellation():
    person_id = 78
    year = 2025
    month = 11
    
    MINIMUM_WAGE = 34.40
    
    with get_conn() as conn:
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        
        print(f"Fetching daily segments for person {person_id}, {month}/{year}...")
        # Pass the DatabaseConnection object 'conn', not the raw 'conn.conn'
        daily_segments, person_name = get_daily_segments_data(
            conn, person_id, year, month, shabbat_cache, MINIMUM_WAGE
        )
        
        print(f"\nAnalyzing Nov 23rd (Sunday)...")
        
        target_day = None
        for day in daily_segments:
            if day["day"] == "23/11/2025":
                target_day = day
                break
        
        if not target_day:
            print("Day 23/11/2025 not found in daily segments!")
            return

        print(f"Day: {target_day['day']}")
        print(f"Shift Names: {target_day['shift_names']}")
        
        if 'cancelled_standbys' in target_day:
            print(f"Cancelled Standbys Count: {len(target_day['cancelled_standbys'])}")
            for i, cs in enumerate(target_day['cancelled_standbys']):
                print(f"  {i+1}. Start: {cs['start']}, End: {cs['end']}, Reason: {cs['reason']}")
        else:
            print("No cancelled standbys.")
            
        # Also print all segments for that day to understand structure
        # Note: We need to modify get_daily_segments_data or inspect internals, 
        # but since we can't easily modify return value without changing code,
        # we'll rely on what's visible.
        # However, we can reconstruct what segments were there by looking at logic.
        
        # Let's peek at the 'buckets' to see work hours
        print("\nBuckets:")
        for k, v in target_day['buckets'].items():
            print(f"  {k}: {v}")

if __name__ == "__main__":
    debug_standby_cancellation()
