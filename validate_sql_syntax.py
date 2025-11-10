#!/usr/bin/env python3
"""
Static SQL Syntax Validation
Checks for common Snowflake SQL syntax errors without connecting to Snowflake
"""

import os
import re
from pathlib import Path
import sys

# Color codes
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'


class SQLValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []

    def check_distinct_in_recursive_cte(self, content, file_path):
        """Check for DISTINCT in recursive CTE terms"""
        # Find recursive CTEs
        cte_pattern = r'WITH\s+RECURSIVE\s+(\w+)\s+AS\s*\((.*?)\)(?=\s*(?:SELECT|,|$))'
        matches = re.finditer(cte_pattern, content, re.DOTALL | re.IGNORECASE)

        for match in matches:
            cte_name = match.group(1)
            cte_body = match.group(2)

            # Check if UNION ALL exists (indicates recursive CTE)
            if re.search(r'UNION\s+ALL', cte_body, re.IGNORECASE):
                # Split on UNION ALL
                parts = re.split(r'UNION\s+ALL', cte_body, flags=re.IGNORECASE)

                if len(parts) >= 2:
                    # Check recursive term (everything after first UNION ALL)
                    for i, recursive_part in enumerate(parts[1:], 1):
                        # Check if SELECT DISTINCT appears in recursive term
                        if re.search(r'SELECT\s+DISTINCT', recursive_part, re.IGNORECASE):
                            self.errors.append({
                                'file': file_path,
                                'error': f"DISTINCT in recursive CTE term '{cte_name}' (part {i+1})",
                                'detail': "DISTINCT is not allowed in recursive terms, only in anchor clause or final SELECT"
                            })

    def check_object_dependencies_columns(self, content, file_path):
        """Check for correct OBJECT_DEPENDENCIES column names"""
        incorrect_columns = [
            (r'referenced_database_name', 'REFERENCED_DATABASE'),
            (r'referenced_schema_name', 'REFERENCED_SCHEMA'),
            (r'referencing_database_name', 'REFERENCING_DATABASE'),
            (r'referencing_schema_name', 'REFERENCING_SCHEMA'),
        ]

        for wrong, correct in incorrect_columns:
            if re.search(wrong, content, re.IGNORECASE):
                self.errors.append({
                    'file': file_path,
                    'error': f"Incorrect OBJECT_DEPENDENCIES column: {wrong}",
                    'detail': f"Should be: {correct}"
                })

    def check_split_to_table_usage(self, content, file_path):
        """Check for correct SPLIT_TO_TABLE usage"""
        # Look for SPLIT_TO_TABLE usage
        if re.search(r'SPLIT_TO_TABLE', content, re.IGNORECASE):
            # Check if it's used with TABLE() function
            if not re.search(r'TABLE\s*\(\s*SPLIT_TO_TABLE', content, re.IGNORECASE):
                self.warnings.append({
                    'file': file_path,
                    'warning': "SPLIT_TO_TABLE should be used with TABLE() function",
                    'detail': "Correct: SELECT VALUE FROM TABLE(SPLIT_TO_TABLE(...))"
                })

            # Check if VALUE column is referenced (it should be)
            if re.search(r'SPLIT_TO_TABLE', content, re.IGNORECASE):
                # This is a basic check - we expect to see VALUE being selected
                if not re.search(r'\.VALUE|SELECT\s+VALUE|TRIM\s*\(\s*VALUE', content, re.IGNORECASE):
                    self.warnings.append({
                        'file': file_path,
                        'warning': "SPLIT_TO_TABLE results should reference VALUE column",
                        'detail': "SPLIT_TO_TABLE returns SEQ, INDEX, VALUE columns"
                    })

    def check_identifier_usage(self, content, file_path):
        """Check for correct IDENTIFIER() usage"""
        # Check for IDENTIFIER usage
        identifier_matches = re.finditer(r'IDENTIFIER\s*\(([^)]+)\)', content, re.IGNORECASE)

        for match in identifier_matches:
            arg = match.group(1).strip()
            # IDENTIFIER should use variables or string concatenation
            if not any(marker in arg for marker in [':', '||', 'CONCAT', "'"]):
                self.warnings.append({
                    'file': file_path,
                    'warning': f"IDENTIFIER usage may be incorrect: IDENTIFIER({arg})",
                    'detail': "IDENTIFIER should use variables (:var) or string concatenation"
                })

    def check_get_ddl_usage(self, content, file_path):
        """Check for correct GET_DDL() usage"""
        # Check for GET_DDL with procedures - should include argument signature
        procedure_ddl_pattern = r"GET_DDL\s*\(\s*['\"]PROCEDURE['\"].*?\)"
        matches = re.finditer(procedure_ddl_pattern, content, re.DOTALL | re.IGNORECASE)

        for match in matches:
            ddl_call = match.group(0)
            # Check if it includes argument signature (parentheses in object name)
            if not re.search(r'\([^)]*\)', ddl_call.split(',', 2)[1] if ',' in ddl_call else ''):
                self.warnings.append({
                    'file': file_path,
                    'warning': "GET_DDL for PROCEDURE should include argument signature",
                    'detail': "Example: GET_DDL('PROCEDURE', 'schema.proc_name(VARCHAR)', TRUE)"
                })

    def check_basic_syntax(self, content, file_path):
        """Check for basic SQL syntax issues"""
        lines = content.split('\n')

        for line_num, line in enumerate(lines, 1):
            # Skip comments
            if line.strip().startswith('--') or not line.strip():
                continue

            # Check for common syntax errors
            # Unmatched quotes (basic check)
            single_quotes = line.count("'") - line.count("\\'")
            if single_quotes % 2 != 0 and not line.strip().endswith('\\'):
                self.warnings.append({
                    'file': file_path,
                    'warning': f"Line {line_num}: Possible unmatched single quotes",
                    'detail': line.strip()[:100]
                })

    def validate_file(self, file_path):
        """Validate a single SQL file"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()

            print(f"\n{BLUE}Validating: {os.path.basename(file_path)}{RESET}")

            # Run all checks
            self.check_distinct_in_recursive_cte(content, file_path)
            self.check_object_dependencies_columns(content, file_path)
            self.check_split_to_table_usage(content, file_path)
            self.check_identifier_usage(content, file_path)
            self.check_get_ddl_usage(content, file_path)
            self.check_basic_syntax(content, file_path)

            # Report file-specific results
            file_errors = [e for e in self.errors if e['file'] == file_path]
            file_warnings = [w for w in self.warnings if w['file'] == file_path]

            if file_errors:
                print(f"{RED}  ✗ {len(file_errors)} error(s) found{RESET}")
                for err in file_errors:
                    print(f"{RED}    ERROR: {err['error']}{RESET}")
                    print(f"           {err['detail']}")
            elif file_warnings:
                print(f"{YELLOW}  ⚠ {len(file_warnings)} warning(s) found{RESET}")
                for warn in file_warnings:
                    print(f"{YELLOW}    WARNING: {warn['warning']}{RESET}")
                    print(f"             {warn['detail']}")
            else:
                print(f"{GREEN}  ✓ No errors or warnings{RESET}")

        except Exception as e:
            print(f"{RED}  ✗ Failed to validate: {str(e)}{RESET}")
            self.errors.append({
                'file': file_path,
                'error': f"File read error: {str(e)}",
                'detail': ''
            })

    def validate_all(self, script_list):
        """Validate all scripts in the list"""
        for script_path in script_list:
            if script_path.exists():
                self.validate_file(script_path)
            else:
                print(f"{RED}✗ File not found: {script_path}{RESET}")
                self.errors.append({
                    'file': str(script_path),
                    'error': 'File not found',
                    'detail': ''
                })

    def print_summary(self):
        """Print validation summary"""
        print(f"\n{BLUE}{'='*80}{RESET}")
        print(f"{BLUE}VALIDATION SUMMARY{RESET}")
        print(f"{BLUE}{'='*80}{RESET}")

        if self.errors:
            print(f"\n{RED}ERRORS: {len(self.errors)}{RESET}")
            for err in self.errors:
                print(f"{RED}  ✗ {os.path.basename(err['file'])}: {err['error']}{RESET}")
                if err['detail']:
                    print(f"    {err['detail']}")

        if self.warnings:
            print(f"\n{YELLOW}WARNINGS: {len(self.warnings)}{RESET}")
            for warn in self.warnings:
                print(f"{YELLOW}  ⚠ {os.path.basename(warn['file'])}: {warn['warning']}{RESET}")
                if warn['detail']:
                    print(f"    {warn['detail']}")

        if not self.errors and not self.warnings:
            print(f"\n{GREEN}✓✓✓ ALL SCRIPTS PASSED STATIC VALIDATION ✓✓✓{RESET}")
            return True
        elif not self.errors:
            print(f"\n{YELLOW}⚠ VALIDATION COMPLETE WITH WARNINGS{RESET}")
            return True
        else:
            print(f"\n{RED}✗✗✗ VALIDATION FAILED - ERRORS FOUND ✗✗✗{RESET}")
            return False


def main():
    """Main validation function"""
    print(f"{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}Static SQL Syntax Validation{RESET}")
    print(f"{BLUE}{'='*80}{RESET}")

    base_path = Path(__file__).parent

    # Define all scripts to validate
    all_scripts = [
        # IMCUST scripts
        base_path / 'IMCUST/MANUAL_01_discovery.sql',
        base_path / 'IMCUST/MANUAL_02_extract_ddl.sql',
        base_path / 'IMCUST/MANUAL_03_create_share.sql',
        base_path / 'IMCUST/MANUAL_04_cleanup.sql',
        base_path / 'IMCUST/AUTOMATED_migration_procedure.sql',
        # IMSDLC scripts
        base_path / 'IMSDLC/MANUAL_01_consume_share.sql',
        base_path / 'IMSDLC/MANUAL_02_create_objects.sql',
        base_path / 'IMSDLC/MANUAL_03_populate_data.sql',
        base_path / 'IMSDLC/MANUAL_04_validate.sql',
        base_path / 'IMSDLC/MANUAL_05_cleanup.sql',
        base_path / 'IMSDLC/AUTOMATED_migration_procedure.sql',
    ]

    validator = SQLValidator()
    validator.validate_all(all_scripts)
    success = validator.print_summary()

    print(f"\n{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}NEXT STEPS:{RESET}")
    print(f"{BLUE}{'='*80}{RESET}")
    print(f"\nTo test with actual Snowflake connection:")
    print(f"  1. Set PAT credentials:")
    print(f"     export IMCUST_PAT='your_imcust_pat_token'")
    print(f"     export IMSDLC_PAT='your_imsdlc_pat_token'")
    print(f"  2. Run connection test:")
    print(f"     python3 test_sql_scripts.py")
    print(f"\n{BLUE}{'='*80}{RESET}")

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
