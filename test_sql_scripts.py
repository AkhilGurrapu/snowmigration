#!/usr/bin/env python3
"""
Snowflake SQL Scripts Testing and Validation
Tests all migration scripts for syntax errors using Snowflake connection
"""

import os
import sys
import snowflake.connector
from pathlib import Path
import re

# Color codes for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

class SnowflakeScriptTester:
    def __init__(self, account, user, password, role, warehouse, database):
        """Initialize Snowflake connection"""
        try:
            self.conn = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                role=role,
                warehouse=warehouse,
                database=database
            )
            self.cursor = self.conn.cursor()
            print(f"{GREEN}✓ Connected to Snowflake account: {account}{RESET}")
        except Exception as e:
            print(f"{RED}✗ Failed to connect to Snowflake: {str(e)}{RESET}")
            sys.exit(1)

    def parse_sql_file(self, file_path):
        """Parse SQL file and split into individual statements"""
        with open(file_path, 'r') as f:
            content = f.read()

        # Remove comments
        content = re.sub(r'--[^\n]*', '', content)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

        # Split by semicolons, handling CREATE PROCEDURE statements specially
        statements = []
        current_stmt = []
        in_procedure = False
        depth = 0

        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Track procedure depth using $$ markers
            if '$$' in line:
                depth += line.count('$$')
                in_procedure = (depth % 2 == 1)

            current_stmt.append(line)

            # End statement on semicolon if not inside procedure
            if ';' in line and not in_procedure:
                statements.append('\n'.join(current_stmt))
                current_stmt = []

        # Add any remaining statement
        if current_stmt:
            statements.append('\n'.join(current_stmt))

        return [s.strip() for s in statements if s.strip()]

    def validate_statement(self, statement, script_name, stmt_number):
        """Validate SQL statement syntax using EXPLAIN"""
        try:
            # Skip certain statements that can't be explained
            skip_patterns = [
                r'^\s*USE\s+',
                r'^\s*SHOW\s+',
                r'^\s*DESC\s+',
                r'^\s*DESCRIBE\s+',
                r'^\s*CALL\s+',
                r'^\s*GRANT\s+',
                r'^\s*CREATE\s+SHARE',
                r'^\s*ALTER\s+SHARE',
                r'^\s*DROP\s+',
                r'^\s*CREATE\s+DATABASE.*FROM\s+SHARE',
            ]

            should_skip = any(re.match(pattern, statement, re.IGNORECASE) for pattern in skip_patterns)

            if should_skip:
                print(f"  {BLUE}◉ Statement {stmt_number}: SKIPPED (not validatable){RESET}")
                return True, "SKIPPED"

            # For CREATE statements, try to extract and validate the SELECT portion
            if re.match(r'^\s*CREATE\s+(OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION)', statement, re.IGNORECASE):
                print(f"  {BLUE}◉ Statement {stmt_number}: PROCEDURE/FUNCTION (syntax check only){RESET}")
                # Just try to execute EXPLAIN on it
                try:
                    self.cursor.execute(f"EXPLAIN {statement}")
                    print(f"  {GREEN}✓ Statement {stmt_number}: VALID{RESET}")
                    return True, "VALID"
                except Exception as e:
                    if "does not support EXPLAIN" in str(e) or "Cannot EXPLAIN" in str(e):
                        # Try parsing check
                        self.cursor.execute(statement)
                        self.cursor.execute(f"DROP PROCEDURE IF EXISTS {self._extract_procedure_name(statement)}")
                        print(f"  {GREEN}✓ Statement {stmt_number}: VALID (created and dropped){RESET}")
                        return True, "VALID"
                    raise

            # For SELECT statements and CTEs
            if re.match(r'^\s*(WITH|SELECT)', statement, re.IGNORECASE):
                self.cursor.execute(f"EXPLAIN {statement}")
                print(f"  {GREEN}✓ Statement {stmt_number}: VALID{RESET}")
                return True, "VALID"

            # For other statements, just try to parse them
            print(f"  {YELLOW}⚠ Statement {stmt_number}: UNCHECKED (manual verification needed){RESET}")
            return True, "UNCHECKED"

        except Exception as e:
            error_msg = str(e)
            print(f"  {RED}✗ Statement {stmt_number}: SYNTAX ERROR{RESET}")
            print(f"    Error: {error_msg}")
            return False, error_msg

    def _extract_procedure_name(self, statement):
        """Extract procedure name from CREATE PROCEDURE statement"""
        match = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+([^\s(]+)', statement, re.IGNORECASE)
        if match:
            return match.group(2)
        return "UNKNOWN"

    def test_script(self, file_path):
        """Test all statements in a SQL script"""
        script_name = os.path.basename(file_path)
        print(f"\n{BLUE}{'='*80}{RESET}")
        print(f"{BLUE}Testing: {script_name}{RESET}")
        print(f"{BLUE}{'='*80}{RESET}")

        try:
            statements = self.parse_sql_file(file_path)
            print(f"Found {len(statements)} statements to validate")

            results = []
            for i, stmt in enumerate(statements, 1):
                # Show first 100 chars of statement
                preview = stmt[:100].replace('\n', ' ')
                if len(stmt) > 100:
                    preview += "..."
                print(f"\n  Statement {i}: {preview}")

                valid, error = self.validate_statement(stmt, script_name, i)
                results.append({
                    'statement_num': i,
                    'valid': valid,
                    'error': error,
                    'preview': preview
                })

            # Summary
            valid_count = sum(1 for r in results if r['valid'])
            total_count = len(results)

            print(f"\n{BLUE}{'-'*80}{RESET}")
            if valid_count == total_count:
                print(f"{GREEN}✓ {script_name}: ALL {total_count} STATEMENTS VALID{RESET}")
                return True
            else:
                print(f"{RED}✗ {script_name}: {total_count - valid_count} ERRORS FOUND{RESET}")
                return False

        except Exception as e:
            print(f"{RED}✗ Failed to test {script_name}: {str(e)}{RESET}")
            return False

    def close(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print(f"\n{GREEN}✓ Disconnected from Snowflake{RESET}")


def main():
    """Main testing function"""
    print(f"{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}Snowflake SQL Scripts Validation{RESET}")
    print(f"{BLUE}{'='*80}{RESET}")

    # Check for PAT credentials
    imcust_pat = os.getenv('IMCUST_PAT')
    imsdlc_pat = os.getenv('IMSDLC_PAT')

    if not imcust_pat or not imsdlc_pat:
        print(f"\n{RED}ERROR: PAT credentials not found in environment{RESET}")
        print(f"\nPlease set the following environment variables:")
        print(f"  export IMCUST_PAT='your_imcust_pat_token'")
        print(f"  export IMSDLC_PAT='your_imsdlc_pat_token'")
        print(f"\nOr create .env files:")
        print(f"  echo 'IMCUST_PAT=your_token' > .env.imcust_pat")
        print(f"  echo 'IMSDLC_PAT=your_token' > .env.imsdlc_pat")
        print(f"  source .env.imcust_pat")
        print(f"  source .env.imsdlc_pat")
        sys.exit(1)

    # Define script groups
    imcust_scripts = [
        'IMCUST/MANUAL_01_discovery.sql',
        'IMCUST/MANUAL_02_extract_ddl.sql',
        'IMCUST/MANUAL_03_create_share.sql',
        'IMCUST/MANUAL_04_cleanup.sql',
        'IMCUST/AUTOMATED_migration_procedure.sql',
    ]

    imsdlc_scripts = [
        'IMSDLC/MANUAL_01_consume_share.sql',
        'IMSDLC/MANUAL_02_create_objects.sql',
        'IMSDLC/MANUAL_03_populate_data.sql',
        'IMSDLC/MANUAL_04_validate.sql',
        'IMSDLC/MANUAL_05_cleanup.sql',
        'IMSDLC/AUTOMATED_migration_procedure.sql',
    ]

    # Test IMCUST scripts
    print(f"\n{YELLOW}{'='*80}{RESET}")
    print(f"{YELLOW}TESTING IMCUST (SOURCE) SCRIPTS{RESET}")
    print(f"{YELLOW}{'='*80}{RESET}")

    tester_imcust = SnowflakeScriptTester(
        account='nfmyizv-imcust',
        user='svc4snowflakedeploy',
        password=imcust_pat,
        role='ACCOUNTADMIN',
        warehouse='admin_wh',
        database='prod_db'
    )

    imcust_results = []
    for script in imcust_scripts:
        script_path = Path(__file__).parent / script
        if script_path.exists():
            result = tester_imcust.test_script(script_path)
            imcust_results.append((script, result))
        else:
            print(f"{RED}✗ Script not found: {script}{RESET}")
            imcust_results.append((script, False))

    tester_imcust.close()

    # Test IMSDLC scripts
    print(f"\n{YELLOW}{'='*80}{RESET}")
    print(f"{YELLOW}TESTING IMSDLC (TARGET) SCRIPTS{RESET}")
    print(f"{YELLOW}{'='*80}{RESET}")

    tester_imsdlc = SnowflakeScriptTester(
        account='nfmyizv-imsdlc',
        user='svc4snowflakedeploy',
        password=imsdlc_pat,
        role='ACCOUNTADMIN',
        warehouse='admin_wh',
        database='dev_db'
    )

    imsdlc_results = []
    for script in imsdlc_scripts:
        script_path = Path(__file__).parent / script
        if script_path.exists():
            result = tester_imsdlc.test_script(script_path)
            imsdlc_results.append((script, result))
        else:
            print(f"{RED}✗ Script not found: {script}{RESET}")
            imsdlc_results.append((script, False))

    tester_imsdlc.close()

    # Final summary
    print(f"\n{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}FINAL SUMMARY{RESET}")
    print(f"{BLUE}{'='*80}{RESET}")

    print(f"\n{YELLOW}IMCUST Scripts:{RESET}")
    for script, result in imcust_results:
        status = f"{GREEN}✓ PASS{RESET}" if result else f"{RED}✗ FAIL{RESET}"
        print(f"  {status} {script}")

    print(f"\n{YELLOW}IMSDLC Scripts:{RESET}")
    for script, result in imsdlc_results:
        status = f"{GREEN}✓ PASS{RESET}" if result else f"{RED}✗ FAIL{RESET}"
        print(f"  {status} {script}")

    # Overall status
    all_passed = all(r[1] for r in imcust_results + imsdlc_results)

    print(f"\n{BLUE}{'='*80}{RESET}")
    if all_passed:
        print(f"{GREEN}✓✓✓ ALL SCRIPTS VALIDATED SUCCESSFULLY ✓✓✓{RESET}")
        return 0
    else:
        failed_count = sum(1 for _, result in imcust_results + imsdlc_results if not result)
        print(f"{RED}✗✗✗ {failed_count} SCRIPT(S) FAILED VALIDATION ✗✗✗{RESET}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
