"""
Database synchronization module for DiyurCalc.
Handles copying data from production database to demo database.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Tables to copy in order (respecting foreign key dependencies)
TABLES_ORDER = [
    "apartment_types",
    "apartments",
    "employers",
    "roles",
    "shift_types",
    "shift_time_segments",
    "standby_rates",
    "minimum_wage_rates",
    "shabbat_times",
    "holidays",
    "payment_codes",
    "payment_component_types",
    "payment_components",
    "people",
    "person_roles",
    "person_apartments",
    "guide_apartments",
    "guide_fixed_payments",
    "time_reports",
    "email_settings",
    "login_logs",
    # History tables
    "person_status_history",
    "apartment_status_history",
    "month_locks",
    "standby_rates_history",
]


def get_demo_connection():
    """Get connection to demo database."""
    demo_url = os.getenv("DEMO_DATABASE_URL")
    if not demo_url:
        raise RuntimeError("DEMO_DATABASE_URL environment variable is required")
    return psycopg2.connect(demo_url)


def get_prod_connection():
    """Get connection to production database."""
    prod_url = os.getenv("DATABASE_URL")
    if not prod_url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return psycopg2.connect(prod_url)


def get_table_columns(conn, table_name: str) -> list[str]:
    """Get column names for a table."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    return [row[0] for row in cur.fetchall()]


def get_table_create_statement(prod_conn, table_name: str) -> str:
    """Generate CREATE TABLE statement from production database."""
    cur = prod_conn.cursor()

    # Get columns with types
    cur.execute("""
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default,
            udt_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cur.fetchall()

    if not columns:
        return None

    # Get primary key
    cur.execute("""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
    """, (table_name,))
    pk_columns = [row[0] for row in cur.fetchall()]

    col_defs = []
    for col in columns:
        col_name, data_type, char_max_len, is_nullable, col_default, udt_name = col

        # Build type string
        if data_type == 'character varying':
            type_str = f"VARCHAR({char_max_len})" if char_max_len else "VARCHAR"
        elif data_type == 'character':
            type_str = f"CHAR({char_max_len})" if char_max_len else "CHAR"
        elif data_type == 'ARRAY':
            type_str = f"{udt_name.replace('_', '')}[]"
        elif data_type == 'USER-DEFINED':
            type_str = udt_name
        else:
            type_str = data_type.upper()

        # Build column definition
        col_def = f'"{col_name}" {type_str}'

        if is_nullable == 'NO':
            col_def += ' NOT NULL'

        if col_default and 'nextval' not in str(col_default):
            col_def += f' DEFAULT {col_default}'
        elif col_default and 'nextval' in str(col_default):
            # Handle serial columns
            if 'integer' in type_str.lower() or 'int' in type_str.lower():
                col_def = f'"{col_name}" SERIAL'
                if is_nullable == 'NO' and col_name not in pk_columns:
                    col_def += ' NOT NULL'

        col_defs.append(col_def)

    # Add primary key constraint
    if pk_columns:
        pk_str = ', '.join(f'"{c}"' for c in pk_columns)
        col_defs.append(f'PRIMARY KEY ({pk_str})')

    return f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  ' + ',\n  '.join(col_defs) + '\n)'


def sync_database(progress_callback=None) -> dict:
    """
    Synchronize demo database with production data.

    Args:
        progress_callback: Optional callback function(step, total, message)

    Returns:
        dict with sync results
    """
    results = {
        "success": True,
        "tables_synced": 0,
        "total_rows": 0,
        "errors": [],
        "details": []
    }

    prod_conn = None
    demo_conn = None

    try:
        logger.info("Starting database sync...")

        prod_conn = get_prod_connection()
        demo_conn = get_demo_connection()
        demo_cur = demo_conn.cursor()

        # Get list of tables in production
        prod_cur = prod_conn.cursor()
        prod_cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            AND table_name NOT LIKE '__drizzle%'
            ORDER BY table_name
        """)
        prod_tables = [row[0] for row in prod_cur.fetchall()]

        # Use ordered list, but include any tables not in the list at the end
        tables_to_sync = []
        for t in TABLES_ORDER:
            if t in prod_tables:
                tables_to_sync.append(t)
        for t in prod_tables:
            if t not in tables_to_sync:
                tables_to_sync.append(t)

        total_steps = len(tables_to_sync) * 2  # Create + Copy for each table
        current_step = 0

        # Step 1: Drop existing tables in demo (reverse order)
        if progress_callback:
            progress_callback(0, total_steps, "מוחק טבלאות קיימות...")

        for table in reversed(tables_to_sync):
            try:
                demo_cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
            except Exception as e:
                logger.warning(f"Could not drop table {table}: {e}")
        demo_conn.commit()

        # Step 2: Create tables and copy data
        for table in tables_to_sync:
            current_step += 1

            if progress_callback:
                progress_callback(current_step, total_steps, f"יוצר טבלה: {table}")

            try:
                # Get and execute CREATE TABLE
                create_sql = get_table_create_statement(prod_conn, table)
                if create_sql:
                    demo_cur.execute(create_sql)
                    demo_conn.commit()
                    logger.info(f"Created table: {table}")

                current_step += 1

                if progress_callback:
                    progress_callback(current_step, total_steps, f"מעתיק נתונים: {table}")

                # Get columns
                columns = get_table_columns(prod_conn, table)
                if not columns:
                    continue

                # Copy data using text mode to handle invalid dates
                prod_cur = prod_conn.cursor()
                col_list = ', '.join(f'"{c}"' for c in columns)

                # Cast timestamp columns to text to avoid Python datetime errors
                select_cols = []
                for c in columns:
                    select_cols.append(f'"{c}"::text as "{c}"')
                select_list = ', '.join(select_cols)

                prod_cur.execute(f'SELECT {select_list} FROM "{table}"')
                raw_rows = prod_cur.fetchall()

                # Convert to list of dicts
                rows = []
                for raw_row in raw_rows:
                    row = {}
                    for i, col in enumerate(columns):
                        row[col] = raw_row[i]
                    rows.append(row)

                if rows:
                    # Insert rows using execute_values for maximum speed
                    insert_sql = f'INSERT INTO "{table}" ({col_list}) VALUES %s'

                    # Prepare all values as tuples
                    all_values = [tuple(row[col] for col in columns) for row in rows]

                    # Use execute_values - sends all data in one query (MUCH faster!)
                    try:
                        execute_values(demo_cur, insert_sql, all_values, page_size=500)
                    except Exception as e:
                        # Fallback to one-by-one if batch fails
                        logger.warning(f"Batch insert failed for {table}, trying one by one: {e}")
                        single_sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({", ".join(["%s"] * len(columns))})'
                        for values in all_values:
                            try:
                                demo_cur.execute(single_sql, values)
                            except Exception as row_err:
                                logger.warning(f"Error inserting row into {table}: {row_err}")

                    demo_conn.commit()

                    # Reset sequence if table has serial column
                    try:
                        demo_cur.execute(f"""
                            SELECT setval(pg_get_serial_sequence('"{table}"', 'id'),
                                   COALESCE((SELECT MAX(id) FROM "{table}"), 1))
                        """)
                        demo_conn.commit()
                    except:
                        pass  # Not all tables have id column

                results["tables_synced"] += 1
                results["total_rows"] += len(rows)
                results["details"].append({
                    "table": table,
                    "rows": len(rows),
                    "status": "success"
                })

                logger.info(f"Synced table {table}: {len(rows)} rows")

            except Exception as e:
                error_msg = f"Error syncing table {table}: {str(e)}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
                results["details"].append({
                    "table": table,
                    "rows": 0,
                    "status": "error",
                    "error": str(e)
                })
                demo_conn.rollback()

        if results["errors"]:
            results["success"] = len(results["errors"]) < len(tables_to_sync) / 2

        logger.info(f"Sync completed. Tables: {results['tables_synced']}, Rows: {results['total_rows']}")

    except Exception as e:
        error_msg = f"Database sync failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        results["success"] = False
        results["errors"].append(error_msg)

    finally:
        if prod_conn:
            prod_conn.close()
        if demo_conn:
            demo_conn.close()

    return results


def check_demo_database_status() -> dict:
    """Check the status of the demo database."""
    try:
        demo_conn = get_demo_connection()
        cur = demo_conn.cursor()

        # Get table count
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            AND table_name NOT LIKE '__drizzle%'
        """)
        tables = [row[0] for row in cur.fetchall()]

        # Get row counts
        table_info = []
        total_rows = 0
        for table in tables:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cur.fetchone()[0]
            total_rows += count
            table_info.append({"table": table, "rows": count})

        demo_conn.close()

        return {
            "connected": True,
            "tables": len(tables),
            "total_rows": total_rows,
            "table_info": table_info
        }

    except Exception as e:
        return {
            "connected": False,
            "error": str(e)
        }


if __name__ == "__main__":
    # Test sync
    logging.basicConfig(level=logging.INFO)

    print("Checking demo database status...")
    status = check_demo_database_status()
    print(f"Status: {status}")

    print("\nStarting sync...")
    result = sync_database(lambda step, total, msg: print(f"[{step}/{total}] {msg}"))
    print(f"\nResult: {result}")
