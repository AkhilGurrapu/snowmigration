#!/usr/bin/env python3
"""
IMCUST - Run Discovery Scripts
Executes discovery and dependency analysis
"""
import os
import snowflake.connector
from dotenv import load_dotenv
import json

# Load from the main .env file location
load_dotenv('/Users/akhilgurrapu/Downloads/snowmigration/.env')

def read_pat_token(file_path):
    """Read PAT token from file"""
    try:
        with open(file_path, 'r') as f:
            return f.read().strip()
    except Exception as e:
        print(f"Error reading PAT token from {file_path}: {e}")
        return None

def execute_discovery(conn, script_path, output_file):
    """Execute discovery script and save results"""
    print(f"\nExecuting: {script_path}")
    print("="*70)
    
    cursor = conn.cursor()
    results = {}
    current_section = None
    
    with open(script_path, 'r') as f:
        sql = f.read()
    
    # Split by section markers
    statements = sql.split(';')
    
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt or stmt.startswith('--'):
            continue
            
        if 'USE ' in stmt.upper()[:10]:
            cursor.execute(stmt)
            continue
            
        # Check if this is a section marker
        if "'===" in stmt and "AS section" in stmt:
            cursor.execute(stmt)
            row = cursor.fetchone()
            if row:
                current_section = row[0]
                results[current_section] = []
                print(f"\n{current_section}")
            continue
        
        try:
            cursor.execute(stmt)
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                if current_section and rows:
                    results[current_section] = {
                        'columns': columns,
                        'rows': [dict(zip(columns, row)) for row in rows]
                    }
                    
                    # Print summary
                    print(f"  Found {len(rows)} records")
                    if len(rows) <= 5:
                        for row in rows:
                            print(f"    {dict(zip(columns, row))}")
                    
        except Exception as e:
            print(f"  Error: {str(e)[:150]}")
    
    cursor.close()
    
    # Save results to JSON
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nResults saved to: {output_file}")
    return results

def main():
    print("\n" + "="*70)
    print("IMCUST - Discovery and Analysis")
    print("="*70)
    
    # Connect to IMCUST
    imcust_pat = read_pat_token('/Users/akhilgurrapu/Downloads/snowmigration/.env.imcust_pat')
    if not imcust_pat:
        print("Failed to read IMCUST PAT token")
        return
        
    conn = snowflake.connector.connect(
        user='svc4snowflakedeploy',
        password=imcust_pat,
        account='nfmyizv-imcust',
        warehouse='admin_wh',
        database='prod_db',
        role='ACCOUNTADMIN'
    )
    
    # Run discovery
    discovery_results = execute_discovery(
        conn, 
        '01_discovery.sql',
        'discovery_results.json'
    )
    
    # Run dependency analysis
    dependency_results = execute_discovery(
        conn,
        '02_dependencies.sql', 
        'dependency_results.json'
    )
    
    conn.close()
    
    # Print summary
    print("\n" + "="*70)
    print("Discovery Summary")
    print("="*70)
    
    # Extract key metrics
    if '=== MIGRATION SUMMARY ===' in discovery_results:
        summary = discovery_results['=== MIGRATION SUMMARY ===']
        if summary and 'rows' in summary:
            for row in summary['rows']:
                print(f"  {row}")
    
    print("\n✅ Discovery completed successfully!")

if __name__ == "__main__":
    main()
