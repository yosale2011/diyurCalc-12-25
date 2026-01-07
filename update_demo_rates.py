
import os
import sys
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.getcwd())

from database import set_demo_mode, get_conn
from history import save_shift_rate_to_history

load_dotenv()

def update_demo_rates():
    print("--- Updating Demo Database Rates for Shift 147 ---")
    set_demo_mode(True)
    with get_conn() as conn:
        cursor = conn.cursor()
        
        # 1. Check current rate
        cursor.execute("SELECT rate, is_minimum_wage FROM shift_types WHERE id = 147")
        row = cursor.fetchone()
        if not row:
            print("Shift 147 not found in Demo DB")
            return
            
        current_rate = row[0]
        is_min_wage = row[1]
        print(f"Current rate in Demo DB: {current_rate}")
        
        # 2. Save 40 NIS to history for < 2026/01
        # We assume the previous rate was 4000 (40 NIS)
        print("Saving historical rate (40 NIS) for < 2026/01...")
        save_shift_rate_to_history(conn, 147, 2026, 1, 4000, False)
        
        # 3. Update current rate to 50 NIS (5000)
        print("Updating current rate to 50 NIS (5000)...")
        cursor.execute("UPDATE shift_types SET rate = 5000, is_minimum_wage = false WHERE id = 147")
        
        conn.commit()
        print("Demo Database updated successfully.")

if __name__ == "__main__":
    update_demo_rates()
