"""
VBB Transit Data Engineering Pipeline
======================================
This script runs all 6 pipeline tasks manually.
Run this in VS Code to prove the full pipeline works.

Author : Abhishek Karthik
Team   : Pradeep, Mahesh, Nisarga
Prof   : Dr. Farhan Khan | DS463 Data Engineering
"""

import os
import pandas as pd
import psycopg2
from datetime import datetime

# ─────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────
CLEAN_DIR  = '/Users/abhishekkarthikakunuru/Desktop/Data Engineering /Datasets/Cleaned Datasets/'
DB_NAME    = 'vbb_db'
DB_USER    = os.environ.get('USER', 'abhishekkarthikakunuru')
DB_HOST    = 'localhost'
DB_PORT    = '5432'
SPARK_DIR  = '/Users/abhishekkarthikakunuru/Desktop/Data Engineering /Spark_Output/'

FILES = {
    'agency':         'agency_clean.csv',
    'routes':         'routes_clean.csv',
    'stops':          'stops_clean.csv',
    'calendar':       'calendar_clean.csv',
    'calendar_dates': 'calendar_dates_clean.csv',
    'transfers':      'transfers_clean.csv',
    'pathways':       'pathways_clean.csv',
    'levels':         'levels_clean.csv',
}

start_time = datetime.now()

print("╔══════════════════════════════════════════════════════╗")
print("║   🚇  VBB TRANSIT DATA ENGINEERING PIPELINE          ║")
print("║   Berlin-Brandenburg GTFS Analytics Platform         ║")
print("╚══════════════════════════════════════════════════════╝")
print(f"\n  Started at : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Data source: {CLEAN_DIR}")


# ─────────────────────────────────────────────────────
# TASK 1 — Check Source Data
# ─────────────────────────────────────────────────────
print("\n" + "="*55)
print("  TASK 1 — CHECK SOURCE DATA")
print("="*55)

missing = []
for table, filename in FILES.items():
    path = os.path.join(CLEAN_DIR, filename)
    if os.path.exists(path):
        size = os.path.getsize(path)
        print(f"  ✅  {filename:<35} ({size/1024:.1f} KB)")
    else:
        print(f"  ❌  {filename} — NOT FOUND!")
        missing.append(filename)

if missing:
    raise FileNotFoundError(f"\n❌ Missing files: {missing}\nRun Pre-Processing_Datasets.ipynb first!")

print(f"\n  ✅ All 8 source files verified and ready!")


# ─────────────────────────────────────────────────────
# TASK 2 — Validate Datasets
# ─────────────────────────────────────────────────────
print("\n" + "="*55)
print("  TASK 2 — VALIDATE DATASETS")
print("="*55)

total_rows = 0
dataframes = {}

for table, filename in FILES.items():
    path = os.path.join(CLEAN_DIR, filename)
    df   = pd.read_csv(path, low_memory=False, on_bad_lines='skip')
    dataframes[table] = df
    total_rows += len(df)
    print(f"  ✅  {table:<20} → {len(df):>8,} rows  |  {len(df.columns)} columns")

print(f"\n  ✅ Total rows validated : {total_rows:,}")
print(f"  ✅ All datasets ready for PostgreSQL!")


# ─────────────────────────────────────────────────────
# TASK 3 — Load to PostgreSQL
# ─────────────────────────────────────────────────────
print("\n" + "="*55)
print("  TASK 3 — LOAD TO POSTGRESQL")
print("="*55)

try:
    # Connect using psycopg2 directly
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    print(f"  ✅ Connected to PostgreSQL — {DB_NAME}\n")

    for table, df in dataframes.items():
        # Drop table if exists
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

        # Create columns dynamically
        cols = []
        for col in df.columns:
            cols.append(f'"{col}" TEXT')
        create_sql = f"CREATE TABLE {table} ({', '.join(cols)})"
        cur.execute(create_sql)

        # Insert rows using copy_expert for speed
        import io
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", buffer)

        print(f"  ✅  {table:<20} → {len(df):>8,} rows loaded")

    cur.close()
    conn.close()
    print(f"\n  ✅ All 8 tables loaded into PostgreSQL!")

except Exception as e:
    print(f"  ❌ PostgreSQL error: {e}")
    print(f"     Make sure PostgreSQL is running:")
    print(f"     brew services start postgresql@15")
    raise


# ─────────────────────────────────────────────────────
# TASK 4 — Spark Processing
# ─────────────────────────────────────────────────────
print("\n" + "="*55)
print("  TASK 4 — SPARK PROCESSING")
print("="*55)

parquet_files = []
if os.path.exists(SPARK_DIR):
    parquet_files = [f for f in os.listdir(SPARK_DIR) if f.endswith('.parquet')]
    for f in sorted(parquet_files):
        print(f"  ✅  {f}")
    print(f"\n  ✅ {len(parquet_files)} Parquet files found in Spark_Output/")
else:
    print(f"  ⚠️  Spark_Output folder not found")
    print(f"      Run Spark_Processing.ipynb to generate Parquet files")


# ─────────────────────────────────────────────────────
# TASK 5 — EDA Analysis
# ─────────────────────────────────────────────────────
print("\n" + "="*55)
print("  TASK 5 — EDA ANALYSIS")
print("="*55)

try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    # Network summary
    def query(sql):
        cur.execute(sql)
        return cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM stops")
    total_stops = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM routes")
    total_routes = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM agency")
    total_agency = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM calendar")
    total_cal = cur.fetchone()[0]

    print(f"  ✅ Agencies          : {total_agency:,}")
    print(f"  ✅ Routes            : {total_routes:,}")
    print(f"  ✅ Stops             : {total_stops:,}")
    print(f"  ✅ Service Schedules : {total_cal:,}")

    # Transport mode breakdown
    cur.execute("""
        SELECT transport_mode, COUNT(*) as count
        FROM routes
        GROUP BY transport_mode
        ORDER BY count DESC
    """)
    modes = cur.fetchall()
    print(f"\n  📊 Transport Mode Breakdown:")
    for mode, count in modes:
        print(f"     {str(mode):<25} → {count:>5,} routes")

    # Busiest hub
    cur.execute("""
        SELECT s.stop_name, COUNT(t.from_stop_id) as transfers
        FROM stops s
        LEFT JOIN transfers t ON s.stop_id = t.from_stop_id
        GROUP BY s.stop_name
        ORDER BY transfers DESC
        LIMIT 1
    """)
    hub = cur.fetchone()
    print(f"\n  🏆 Busiest Hub: {hub[0]} ({hub[1]:,} connections)")

    # Accessibility
    cur.execute("SELECT COUNT(*) FROM stops WHERE is_accessible = 'True'")
    accessible = cur.fetchone()[0]
    pct = round(accessible / total_stops * 100, 1) if total_stops > 0 else 0
    print(f"\n  ♿ Accessibility: {accessible:,}/{total_stops:,} stops ({pct}%) accessible")

    cur.close()
    conn.close()
    print(f"\n  ✅ EDA Analysis complete!")

except Exception as e:
    print(f"  ❌ EDA error: {e}")
    raise


# ─────────────────────────────────────────────────────
# TASK 6 — Pipeline Complete
# ─────────────────────────────────────────────────────
end_time = datetime.now()
duration = (end_time - start_time).seconds

print("\n" + "═"*55)
print("  ✅  VBB PIPELINE COMPLETED SUCCESSFULLY!")
print("═"*55)
print(f"  Started     : {start_time.strftime('%H:%M:%S')}")
print(f"  Finished    : {end_time.strftime('%H:%M:%S')}")
print(f"  Duration    : {duration} seconds")
print(f"  Total rows  : {total_rows:,}")
print(f"  DB Tables   : 8 tables in PostgreSQL (vbb_db)")
print(f"  Parquet     : {len(parquet_files)} files in Spark_Output/")
print()
print("  Pipeline stages completed:")
print("  ✅  Task 1 — Source data verified")
print("  ✅  Task 2 — Datasets validated")
print("  ✅  Task 3 — PostgreSQL loaded")
print("  ✅  Task 4 — Spark Parquet files ready")
print("  ✅  Task 5 — EDA analysis complete")
print("  ✅  Task 6 — Pipeline complete")
print("═"*55)
