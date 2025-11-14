-- ============================================
-- IMCUST (SOURCE) - Create Test Dataset V3 (Completely Different Objects)
-- ============================================
-- Purpose: Create a NEW test dataset with different objects to test:
--   1. Complex multi-level dependencies
--   2. Views with nested dependencies
--   3. Tables with MERGE statements
--   4. Tables with CTAS that need conversion
--   5. Tables with no query history (tests all fallback strategies)
--   6. Different data types and transformations
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS SRC_HR_BOLT;
CREATE SCHEMA IF NOT EXISTS MART_HR_BOLT;

USE SCHEMA SRC_HR_BOLT;

-- ============================================
-- BASE TABLES (Level 3 - deepest dependencies)
-- ============================================

-- Table 1: Employee Master (base table)
CREATE OR REPLACE TABLE employee_master (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    hire_date DATE,
    department_id INT,
    salary DECIMAL(10,2),
    manager_id INT,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO employee_master VALUES
(1001, 'John', 'Doe', 'john.doe@company.com', '2020-01-15', 10, 75000.00, NULL, CURRENT_TIMESTAMP()),
(1002, 'Jane', 'Smith', 'jane.smith@company.com', '2020-03-20', 20, 85000.00, 1001, CURRENT_TIMESTAMP()),
(1003, 'Bob', 'Johnson', 'bob.johnson@company.com', '2021-06-10', 10, 70000.00, 1001, CURRENT_TIMESTAMP()),
(1004, 'Alice', 'Williams', 'alice.williams@company.com', '2021-08-25', 30, 90000.00, 1002, CURRENT_TIMESTAMP()),
(1005, 'Charlie', 'Brown', 'charlie.brown@company.com', '2022-02-14', 20, 80000.00, 1002, CURRENT_TIMESTAMP());

-- Table 2: Department Master (base table)
CREATE OR REPLACE TABLE department_master (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100),
    location VARCHAR(50),
    budget DECIMAL(15,2),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO department_master VALUES
(10, 'Engineering', 'San Francisco', 5000000.00, CURRENT_TIMESTAMP()),
(20, 'Sales', 'New York', 3000000.00, CURRENT_TIMESTAMP()),
(30, 'Marketing', 'Los Angeles', 2000000.00, CURRENT_TIMESTAMP()),
(40, 'Finance', 'Chicago', 1500000.00, CURRENT_TIMESTAMP());

-- Table 3: Project Master (base table)
CREATE OR REPLACE TABLE project_master (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(15,2),
    status VARCHAR(20),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO project_master VALUES
(501, 'Project Alpha', '2023-01-01', '2023-12-31', 1000000.00, 'ACTIVE', CURRENT_TIMESTAMP()),
(502, 'Project Beta', '2023-03-15', '2024-03-14', 1500000.00, 'ACTIVE', CURRENT_TIMESTAMP()),
(503, 'Project Gamma', '2022-06-01', '2023-05-31', 800000.00, 'COMPLETED', CURRENT_TIMESTAMP()),
(504, 'Project Delta', '2023-09-01', '2024-08-31', 1200000.00, 'ACTIVE', CURRENT_TIMESTAMP());

-- Table 4: Time Tracking Raw (base table)
CREATE OR REPLACE TABLE time_tracking_raw (
    time_entry_id INT PRIMARY KEY,
    employee_id INT,
    project_id INT,
    work_date DATE,
    hours_worked DECIMAL(5,2),
    task_description VARCHAR(200),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO time_tracking_raw VALUES
(2001, 1001, 501, '2024-01-15', 8.00, 'Development work', CURRENT_TIMESTAMP()),
(2002, 1002, 501, '2024-01-15', 7.50, 'Code review', CURRENT_TIMESTAMP()),
(2003, 1003, 502, '2024-01-16', 8.00, 'Testing', CURRENT_TIMESTAMP()),
(2004, 1004, 503, '2024-01-16', 6.00, 'Documentation', CURRENT_TIMESTAMP()),
(2005, 1005, 504, '2024-01-17', 8.00, 'Client meeting', CURRENT_TIMESTAMP()),
(2006, 1001, 501, '2024-01-17', 8.00, 'Development work', CURRENT_TIMESTAMP()),
(2007, 1002, 502, '2024-01-18', 7.00, 'Design work', CURRENT_TIMESTAMP());

-- ============================================
-- DERIVED TABLES (Level 2 - have transformations)
-- ============================================

USE SCHEMA MART_HR_BOLT;

-- Table 1: Dim Employees (derived from employee_master)
-- This will have query history captured
CREATE OR REPLACE TABLE dim_employees (
    employee_key INT PRIMARY KEY,
    employee_id INT,
    full_name VARCHAR(101),
    email VARCHAR(100),
    hire_date DATE,
    department_id INT,
    salary DECIMAL(10,2),
    manager_id INT,
    years_of_service INT,
    is_active BOOLEAN,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert using transformation SQL (will be captured in query history)
INSERT INTO dim_employees
SELECT
    employee_id as employee_key,
    employee_id,
    first_name || ' ' || last_name as full_name,
    email,
    hire_date,
    department_id,
    salary,
    manager_id,
    DATEDIFF('YEAR', hire_date, CURRENT_DATE()) as years_of_service,
    TRUE as is_active,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_HR_BOLT.employee_master;

-- Table 2: Dim Departments (derived from department_master)
-- This will have MULTIPLE query histories (we'll insert twice)
CREATE OR REPLACE TABLE dim_departments (
    department_key INT PRIMARY KEY,
    department_id INT,
    department_name VARCHAR(100),
    location VARCHAR(50),
    budget DECIMAL(15,2),
    budget_category VARCHAR(20),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- First insert (older - will be ignored)
INSERT INTO dim_departments
SELECT
    department_id as department_key,
    department_id,
    department_name,
    location,
    budget,
    CASE
        WHEN budget > 4000000 THEN 'HIGH'
        WHEN budget > 2000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END as budget_category,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_HR_BOLT.department_master
WHERE budget > 2000000;

-- Second insert (newer - will be selected by framework)
INSERT INTO dim_departments
SELECT
    department_id as department_key,
    department_id,
    department_name,
    location,
    budget,
    CASE
        WHEN budget > 4000000 THEN 'HIGH'
        WHEN budget > 2000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END as budget_category,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_HR_BOLT.department_master;

-- Table 3: Fact Time Entries (complex transformation with joins - will have query history)
CREATE OR REPLACE TABLE fact_time_entries (
    fact_time_entry_id INT PRIMARY KEY,
    time_entry_id INT,
    employee_key INT,
    department_key INT,
    project_id INT,
    work_date DATE,
    hours_worked DECIMAL(5,2),
    cost_amount DECIMAL(10,2),
    task_description VARCHAR(200),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Complex transformation with joins and calculations
INSERT INTO fact_time_entries
SELECT
    ttr.time_entry_id as fact_time_entry_id,
    ttr.time_entry_id,
    de.employee_key,
    dd.department_key,
    ttr.project_id,
    ttr.work_date,
    ttr.hours_worked,
    ROUND(ttr.hours_worked * (de.salary / 2080), 2) as cost_amount,
    ttr.task_description,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_HR_BOLT.time_tracking_raw ttr
JOIN dim_employees de ON ttr.employee_id = de.employee_id
JOIN dim_departments dd ON de.department_id = dd.department_id;

-- Table 4: Employee Performance Summary (will use MERGE - tests MERGE capture)
CREATE OR REPLACE TABLE employee_performance_summary (
    employee_key INT PRIMARY KEY,
    employee_id INT,
    total_hours DECIMAL(10,2),
    total_projects INT,
    avg_hours_per_day DECIMAL(5,2),
    last_work_date DATE,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Initial insert
INSERT INTO employee_performance_summary
SELECT
    de.employee_key,
    de.employee_id,
    0.00 as total_hours,
    0 as total_projects,
    0.00 as avg_hours_per_day,
    NULL as last_work_date,
    CURRENT_TIMESTAMP() as created_ts,
    CURRENT_TIMESTAMP() as updated_ts
FROM dim_employees de;

-- MERGE statement (will be captured)
MERGE INTO employee_performance_summary eps
USING (
    SELECT
        de.employee_key,
        de.employee_id,
        SUM(fte.hours_worked) as total_hours,
        COUNT(DISTINCT fte.project_id) as total_projects,
        ROUND(AVG(fte.hours_worked), 2) as avg_hours_per_day,
        MAX(fte.work_date) as last_work_date
    FROM dim_employees de
    LEFT JOIN fact_time_entries fte ON de.employee_key = fte.employee_key
    GROUP BY de.employee_key, de.employee_id
) src
ON eps.employee_key = src.employee_key
WHEN MATCHED THEN
    UPDATE SET
        total_hours = src.total_hours,
        total_projects = src.total_projects,
        avg_hours_per_day = src.avg_hours_per_day,
        last_work_date = src.last_work_date,
        updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (employee_key, employee_id, total_hours, total_projects, avg_hours_per_day, last_work_date, created_ts, updated_ts)
    VALUES (src.employee_key, src.employee_id, src.total_hours, src.total_projects, src.avg_hours_per_day, src.last_work_date, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Table 5: Department Budget Summary (will have NO query history - tests fallback)
-- We'll create it but NOT populate it via INSERT (simulates missing history)
CREATE OR REPLACE TABLE department_budget_summary (
    summary_id INT PRIMARY KEY,
    department_key INT,
    department_name VARCHAR(100),
    total_employees INT,
    total_salary_cost DECIMAL(15,2),
    budget_utilization_pct DECIMAL(5,2),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Manually populate (simulates data loaded via external tool, no query history)
INSERT INTO department_budget_summary VALUES
(1, 10, 'Engineering', 2, 145000.00, 2.90, CURRENT_TIMESTAMP()),
(2, 20, 'Sales', 2, 165000.00, 5.50, CURRENT_TIMESTAMP()),
(3, 30, 'Marketing', 1, 90000.00, 4.50, CURRENT_TIMESTAMP());

-- Add COMMENT to department_budget_summary (tests fallback Strategy 2)
COMMENT ON TABLE department_budget_summary IS 
'INSERT INTO department_budget_summary
SELECT
    ROW_NUMBER() OVER (ORDER BY dd.department_key) as summary_id,
    dd.department_key,
    dd.department_name,
    COUNT(DISTINCT de.employee_key) as total_employees,
    SUM(de.salary) as total_salary_cost,
    ROUND((SUM(de.salary) / dd.budget) * 100, 2) as budget_utilization_pct,
    CURRENT_TIMESTAMP() as created_ts
FROM dim_departments dd
LEFT JOIN dim_employees de ON dd.department_id = de.department_id
GROUP BY dd.department_key, dd.department_name, dd.budget';

-- Table 6: Project Allocation (will use CTAS - tests CTAS to INSERT conversion)
CREATE OR REPLACE TABLE project_allocation AS
SELECT
    pm.project_id,
    pm.project_name,
    pm.status,
    COUNT(DISTINCT fte.employee_key) as allocated_employees,
    SUM(fte.hours_worked) as total_hours_allocated,
    ROUND(AVG(fte.hours_worked), 2) as avg_hours_per_employee,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_HR_BOLT.project_master pm
LEFT JOIN fact_time_entries fte ON pm.project_id = fte.project_id
GROUP BY pm.project_id, pm.project_name, pm.status;

-- ============================================
-- VIEWS (Level 1 - depend on tables)
-- ============================================

CREATE OR REPLACE VIEW vw_employee_details AS
SELECT
    de.employee_key,
    de.full_name,
    de.email,
    dd.department_name,
    dd.location,
    de.salary,
    de.years_of_service,
    eps.total_hours,
    eps.total_projects
FROM dim_employees de
JOIN dim_departments dd ON de.department_id = dd.department_id
LEFT JOIN employee_performance_summary eps ON de.employee_key = eps.employee_key;

CREATE OR REPLACE VIEW vw_project_team AS
SELECT
    pm.project_id,
    pm.project_name,
    pm.status,
    COUNT(DISTINCT fte.employee_key) as team_size,
    SUM(fte.hours_worked) as total_project_hours,
    ROUND(SUM(fte.cost_amount), 2) as total_project_cost
FROM PROD_DB.SRC_HR_BOLT.project_master pm
LEFT JOIN fact_time_entries fte ON pm.project_id = fte.project_id
GROUP BY pm.project_id, pm.project_name, pm.status;

CREATE OR REPLACE VIEW vw_department_utilization AS
SELECT
    dd.department_key,
    dd.department_name,
    COUNT(DISTINCT de.employee_key) as employee_count,
    SUM(de.salary) as total_salary,
    dd.budget,
    ROUND((SUM(de.salary) / dd.budget) * 100, 2) as utilization_pct
FROM dim_departments dd
LEFT JOIN dim_employees de ON dd.department_id = de.department_id
GROUP BY dd.department_key, dd.department_name, dd.budget;

-- ============================================
-- Summary
-- ============================================

SELECT 'SRC_HR_BOLT' as schema_name, 'employee_master' as table_name, COUNT(*) as row_count FROM PROD_DB.SRC_HR_BOLT.employee_master
UNION ALL
SELECT 'SRC_HR_BOLT', 'department_master', COUNT(*) FROM PROD_DB.SRC_HR_BOLT.department_master
UNION ALL
SELECT 'SRC_HR_BOLT', 'project_master', COUNT(*) FROM PROD_DB.SRC_HR_BOLT.project_master
UNION ALL
SELECT 'SRC_HR_BOLT', 'time_tracking_raw', COUNT(*) FROM PROD_DB.SRC_HR_BOLT.time_tracking_raw
UNION ALL
SELECT 'MART_HR_BOLT', 'dim_employees', COUNT(*) FROM PROD_DB.MART_HR_BOLT.dim_employees
UNION ALL
SELECT 'MART_HR_BOLT', 'dim_departments', COUNT(*) FROM PROD_DB.MART_HR_BOLT.dim_departments
UNION ALL
SELECT 'MART_HR_BOLT', 'fact_time_entries', COUNT(*) FROM PROD_DB.MART_HR_BOLT.fact_time_entries
UNION ALL
SELECT 'MART_HR_BOLT', 'employee_performance_summary', COUNT(*) FROM PROD_DB.MART_HR_BOLT.employee_performance_summary
UNION ALL
SELECT 'MART_HR_BOLT', 'department_budget_summary', COUNT(*) FROM PROD_DB.MART_HR_BOLT.department_budget_summary
UNION ALL
SELECT 'MART_HR_BOLT', 'project_allocation', COUNT(*) FROM PROD_DB.MART_HR_BOLT.project_allocation
ORDER BY schema_name, table_name;

