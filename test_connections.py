#!/usr/bin/env python3
"""
Test Snowflake connections for both IMCUST and IMSDLC accounts
"""
import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_connection(account_name, account_id, user, warehouse, database, pat_env_var):
    """Test a Snowflake connection and return results"""
    print(f"\n{'='*60}")
    print(f"Testing {account_name} connection...")
    print(f"{'='*60}")

    pat_token = os.getenv(pat_env_var)
    if not pat_token:
        print(f"❌ ERROR: {pat_env_var} not found in environment variables")
        return False

    print(f"✓ PAT token loaded from environment")
    print(f"  Account: {account_id}")
    print(f"  User: {user}")
    print(f"  Database: {database}")
    print(f"  Warehouse: {warehouse}")

    try:
        # Connect using PAT token as password
        conn = snowflake.connector.connect(
            user=user,
            password=pat_token,  # PAT token goes in password parameter
            account=account_id,
            warehouse=warehouse,
            database=database,
            schema='INFORMATION_SCHEMA',
            role='ACCOUNTADMIN'
        )

        print(f"✓ Connection established successfully")

        # Execute a simple query to verify connection
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
        result = cursor.fetchone()

        print(f"\n📊 Connection Details:")
        print(f"  Account: {result[0]}")
        print(f"  User: {result[1]}")
        print(f"  Role: {result[2]}")
        print(f"  Warehouse: {result[3]}")
        print(f"  Database: {result[4]}")

        # Test warehouse access
        cursor.execute("SHOW WAREHOUSES LIKE 'ADMIN_WH'")
        warehouses = cursor.fetchall()
        if warehouses:
            print(f"✓ Warehouse 'ADMIN_WH' is accessible")

        # List schemas in database
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        schemas = cursor.fetchall()
        print(f"\n📁 Schemas in {database}: {len(schemas)} found")
        for schema in schemas[:5]:  # Show first 5 schemas
            print(f"  - {schema[1]}")
        if len(schemas) > 5:
            print(f"  ... and {len(schemas) - 5} more")

        cursor.close()
        conn.close()

        print(f"\n✅ {account_name} connection test PASSED")
        return True

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"\n❌ {account_name} connection test FAILED")
        print(f"Error Code: {e.errno}")
        print(f"Error Message: {e.msg}")
        return False
    except Exception as e:
        print(f"\n❌ {account_name} connection test FAILED")
        print(f"Error: {str(e)}")
        return False

def main():
    print("\n" + "="*60)
    print("Snowflake Cross-Account Connection Tests")
    print("="*60)

    # Test IMCUST (Production)
    imcust_success = test_connection(
        account_name="IMCUST (Production)",
        account_id="nfmyizv-imcust",
        user="svc4snowflakedeploy",
        warehouse="admin_wh",
        database="prod_db",
        pat_env_var="IMCUST_PAT"
    )

    # Test IMSDLC (Development)
    imsdlc_success = test_connection(
        account_name="IMSDLC (Development)",
        account_id="nfmyizv-imsdlc",
        user="svc4snowflakedeploy",
        warehouse="admin_wh",
        database="dev_db",
        pat_env_var="IMSDLC_PAT"
    )

    # Test ADMIN (Administrative)
    admin_success = test_connection(
        account_name="ADMIN (Administrative)",
        account_id="nfmyizv-lib31145",
        user="svc4snowflakedeploy",
        warehouse="admin_wh",
        database="admin_db",
        pat_env_var="ADMIN_PAT"
    )

    # Summary
    print(f"\n{'='*60}")
    print("Connection Test Summary")
    print(f"{'='*60}")
    print(f"IMCUST (Production):    {'✅ PASSED' if imcust_success else '❌ FAILED'}")
    print(f"IMSDLC (Development):   {'✅ PASSED' if imsdlc_success else '❌ FAILED'}")
    print(f"ADMIN (Administrative): {'✅ PASSED' if admin_success else '❌ FAILED'}")

    if imcust_success and imsdlc_success and admin_success:
        print(f"\n🎉 All connections successful! Ready for migration.")
    else:
        print(f"\n⚠️  Some connections failed. Please check credentials and network access.")

    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
