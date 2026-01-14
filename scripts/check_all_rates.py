
import os
import sys
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.getcwd())

from core.database import set_demo_mode, get_conn
from core.history import get_shift_rate_for_month

load_dotenv()

def check_rates():
    print("--- Checking Rates for Shift 147 ---")
    
    # Production
    print("\n[Production Database]")
    set_demo_mode(False)
    with get_conn() as conn:
        rate_12_25 = get_shift_rate_for_month(conn, 147, 2025, 12)
        rate_01_26 = get_shift_rate_for_month(conn, 147, 2026, 1)
        print(f"Dec 2025: {rate_12_25}")
        print(f"Jan 2026: {rate_01_26}")
        
        cursor = conn.cursor()
        cursor.execute("SELECT rate FROM shift_types WHERE id = 147")
        print(f"Current rate in shift_types: {cursor.fetchone()[0]}")

    # Demo
    print("\n[Demo Database]")
    set_demo_mode(True)
    with get_conn() as conn:
        rate_12_25 = get_shift_rate_for_month(conn, 147, 2025, 12)
        rate_01_26 = get_shift_rate_for_month(conn, 147, 2026, 1)
        print(f"Dec 2025: {rate_12_25}")
        print(f"Jan 2026: {rate_01_26}")
        
        cursor = conn.cursor()
        cursor.execute("SELECT rate FROM shift_types WHERE id = 147")
        print(f"Current rate in shift_types: {cursor.fetchone()[0]}")

if __name__ == "__main__":
    check_rates()
