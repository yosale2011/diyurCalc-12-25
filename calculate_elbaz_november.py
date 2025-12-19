"""
חישוב שעות עבודה של אלבז אריאל לחודש נובמבר 2025
"""
import psycopg2
import psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv
import os

# טעינת משתני סביבה
load_dotenv()
DB_CONNECTION_STRING = os.getenv("DATABASE_URL")

def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    return conn

def calculate_elbaz_november():
    """חישוב שעות עבודה של אלבז אריאל לנובמבר 2025"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        # מציאת אלבז אריאל
        cursor.execute("SELECT id, name FROM people WHERE name LIKE %s", ('%אלבז אריאל%',))
        person = cursor.fetchone()
        
        if not person:
            print("לא נמצא אלבז אריאל במערכת")
            return
            
        person_id = person['id']
        print(f"נמצא: {person['name']} (ID: {person_id})")
        
        # שליפת דיווחים לנובמבר 2025
        start_date = datetime(2025, 11, 1).date()
        end_date = datetime(2025, 12, 1).date()
        
        cursor.execute("""
            SELECT tr.*, st.name as shift_name,
                   a.apartment_type_id,
                   p.is_married
            FROM time_reports tr
            LEFT JOIN shift_types st ON st.id = tr.shift_type_id
            LEFT JOIN apartments a ON tr.apartment_id = a.id
            LEFT JOIN people p ON tr.person_id = p.id
            WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
            ORDER BY tr.date, tr.start_time
        """, (person_id, start_date, end_date))
        
        reports = cursor.fetchall()
        print(f"\nנמצאו {len(reports)} דיווחים לנובמבר 2025")
        
        if not reports:
            print("אין דיווחים לחודש נובמבר")
            return
        
        # שליפת מקטעי זמן של משמרות
        shift_ids = {r["shift_type_id"] for r in reports if r["shift_type_id"]}
        segments_by_shift = {}
        
        if shift_ids:
            placeholders = ",".join(["%s"] * len(shift_ids))
            cursor.execute(
                f"""SELECT id, shift_type_id, start_time, end_time, wage_percent, segment_type, order_index
                    FROM shift_time_segments
                    WHERE shift_type_id IN ({placeholders})
                    ORDER BY order_index""",
                tuple(shift_ids)
            )
            segs = cursor.fetchall()
            for s in segs:
                segments_by_shift.setdefault(s["shift_type_id"], []).append(s)
        
        # הפעלת פונקציית החישוב המלאה
        from logic import calculate_person_monthly_totals, get_shabbat_times_cache
        
        shabbat_cache = get_shabbat_times_cache(conn)
        totals = calculate_person_monthly_totals(
            conn, person_id, 2025, 11, shabbat_cache, minimum_wage=34.40
        )
        
        # הצגת תוצאות
        print("\n=== סיכום שעות עבודה לפי אחוזים ===")
        print(f"שעות ב-100%: {totals.get('calc100', 0) / 60:.2f}")
        print(f"שעות ב-125%: {totals.get('calc125', 0) / 60:.2f}")
        print(f"שעות ב-150%: {totals.get('calc150', 0) / 60:.2f}")
        print(f"שעות ב-175%: {totals.get('calc175', 0) / 60:.2f}")
        print(f"שעות ב-200%: {totals.get('calc200', 0) / 60:.2f}")
        
        # חישוב תשלום לפי תעריפים
        min_wage = 34.40  # שכר מינימום לפי שעה
        
        print("\n=== תשלום לפי תעריפים ===")
        payment_100 = (totals.get('calc100', 0) / 60) * min_wage * 1.0
        payment_125 = (totals.get('calc125', 0) / 60) * min_wage * 1.25
        payment_150 = (totals.get('calc150', 0) / 60) * min_wage * 1.5
        payment_175 = (totals.get('calc175', 0) / 60) * min_wage * 1.75
        payment_200 = (totals.get('calc200', 0) / 60) * min_wage * 2.0
        
        print(f"תשלום עבור שעות 100%: ₪{payment_100:,.2f}")
        print(f"תשלום עבור שעות 125%: ₪{payment_125:,.2f}")
        print(f"תשלום עבור שעות 150%: ₪{payment_150:,.2f}")
        print(f"תשלום עבור שעות 175%: ₪{payment_175:,.2f}")
        print(f"תשלום עבור שעות 200%: ₪{payment_200:,.2f}")
        
        total_payment = payment_100 + payment_125 + payment_150 + payment_175 + payment_200
        print(f"\nסה\"כ תשלום עבודה (בלי כוננות): ₪{total_payment:,.2f}")
        print(f"תוספת כוננות: ₪{totals.get('standby_payment', 0):,.2f}")
        print(f"סה\"כ כולל כוננות: ₪{total_payment + totals.get('standby_payment', 0):,.2f}")
        
        print("\n=== פירוט נוסף ===")
        print(f"סה\"כ שעות עבודה: {totals.get('total_hours', 0) / 60:.2f}")
        print(f"תשלום כוננות: ₪{totals.get('standby_payment', 0):.2f}")
        print(f"ימי עבודה בפועל: {totals.get('actual_work_days', 0)}")
        
    except Exception as e:
        print(f"שגיאה: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    calculate_elbaz_november()
