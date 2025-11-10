#!/usr/bin/env python3
"""
IMSDLC - Execute Complete Migration
Runs all migration scripts in order
"""
import os
import snowflake.connector
from dotenv import load_dotenv
import time

load_dotenv()

def execute_sql_file(conn, filepath, desc):
    """Execute SQL file and report results"""
    print(f"\n{'='*70}")
    print(f"Executing: {desc}")
    print(f"{'='*70}")

    cursor = conn.cursor()

    with open(filepath, 'r') as f:
        sql = f.read()

    for stmt in sql.split(';'):
        stmt = stmt.strip()
        if not stmt or stmt.startswith('--') or 'USE ' in stmt.upper()[:10]:
            if 'USE ' in stmt.upper()[:10]:
                cursor.execute(stmt)
            continue

        try:
            cursor.execute(stmt)

            # Check if there are results to fetch
            if cursor.description:
                results = cursor.fetchall()
                if results and len(results) <= 10:
                    columns = [desc[0] for desc in cursor.description]
                    print(f"  Columns: {columns}")
                    for row in results[:5]:
                        print(f"    {dict(zip(columns, row))}")
                elif results:
                    print(f"  ✓ Query returned {len(results)} rows")
            elif 'INSERT' in stmt.upper()[:20]:
                print(f"  ✓ Inserted {cursor.rowcount} rows")
            elif 'CREATE' in stmt.upper()[:20]:
                print(f"  ✓ Object created")
            elif 'CALL' in stmt.upper()[:10]:
                result = cursor.fetchone()
                print(f"  ✓ Procedure result: {result[0] if result else 'Success'}")

        except Exception as e:
            error_msg = str(e)
            if 'already exists' not in error_msg.lower():
                print(f"  ⚠ {error_msg[:150]}")

    cursor.close()

def main():
    print("\n" + "="*70)
    print("IMSDLC - Complete Migration Execution")
    print("="*70)

    conn = snowflake.connector.connect(
        user='svc4snowflakedeploy',
        password=os.getenv('IMSDLC_PAT'),
        account='nfmyizv-imsdlc',
        warehouse='admin_wh',
        database='dev_db',
        role='ACCOUNTADMIN'
    )

    # Step 1: Consume Share
    execute_sql_file(conn, '01_consume_share.sql', 'Step 1: Consume Share from IMCUST')
    time.sleep(2)

    # Step 2: Create Objects
    execute_sql_file(conn, '02_create_objects.sql', 'Step 2: Create Objects (DDL)')
    time.sleep(2)

    # Step 3: Populate Data
    execute_sql_file(conn, '03_populate_data.sql', 'Step 3: Populate Data from Share')
    time.sleep(2)

    # Step 4: Validate
    execute_sql_file(conn, '04_validate.sql', 'Step 4: Validation and Testing')

    conn.close()

    print("\n" + "="*70)
    print("✅ Complete Migration Finished")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
