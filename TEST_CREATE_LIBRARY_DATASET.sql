-- ============================================
-- CREATE COMPREHENSIVE LIBRARY TEST DATASET
-- ============================================
-- Purpose: Create 10 tables (facts + dimensions) and 3 views
--          Across both schemas for comprehensive migration testing
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;

-- ============================================
-- SCHEMA 1: SRC_INVESTMENTS_BOLT (Source/Raw Layer)
-- ============================================
USE SCHEMA src_investments_bolt;

-- Dimension Tables (Source Layer)
CREATE OR REPLACE TABLE dim_library_branches (
    branch_id NUMBER PRIMARY KEY,
    branch_name VARCHAR(100),
    branch_address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    phone VARCHAR(20),
    created_date DATE DEFAULT CURRENT_DATE()
);

CREATE OR REPLACE TABLE dim_books (
    book_id NUMBER PRIMARY KEY,
    isbn VARCHAR(20),
    title VARCHAR(200),
    author VARCHAR(100),
    publisher VARCHAR(100),
    publication_year NUMBER,
    genre VARCHAR(50),
    language VARCHAR(20),
    created_date DATE DEFAULT CURRENT_DATE()
);

CREATE OR REPLACE TABLE dim_members (
    member_id NUMBER PRIMARY KEY,
    member_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    membership_type VARCHAR(20),
    join_date DATE,
    created_date DATE DEFAULT CURRENT_DATE()
);

CREATE OR REPLACE TABLE dim_authors (
    author_id NUMBER PRIMARY KEY,
    author_name VARCHAR(100),
    nationality VARCHAR(50),
    birth_year NUMBER,
    created_date DATE DEFAULT CURRENT_DATE()
);

-- Fact Tables (Source Layer)
CREATE OR REPLACE TABLE fact_book_inventory (
    inventory_id NUMBER PRIMARY KEY,
    book_id NUMBER,
    branch_id NUMBER,
    total_copies NUMBER,
    available_copies NUMBER,
    reserved_copies NUMBER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (book_id) REFERENCES dim_books(book_id),
    FOREIGN KEY (branch_id) REFERENCES dim_library_branches(branch_id)
);

CREATE OR REPLACE TABLE fact_book_loans (
    loan_id NUMBER PRIMARY KEY,
    member_id NUMBER,
    book_id NUMBER,
    branch_id NUMBER,
    loan_date DATE,
    due_date DATE,
    return_date DATE,
    fine_amount NUMBER(10,2),
    status VARCHAR(20),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (member_id) REFERENCES dim_members(member_id),
    FOREIGN KEY (book_id) REFERENCES dim_books(book_id),
    FOREIGN KEY (branch_id) REFERENCES dim_library_branches(branch_id)
);

-- ============================================
-- SCHEMA 2: MART_INVESTMENTS_BOLT (Mart/Analytics Layer)
-- ============================================
USE SCHEMA mart_investments_bolt;

-- Dimension Tables (Mart Layer - may reference source)
CREATE OR REPLACE TABLE dim_book_categories (
    category_id NUMBER PRIMARY KEY,
    category_name VARCHAR(50),
    parent_category_id NUMBER,
    description VARCHAR(200),
    created_date DATE DEFAULT CURRENT_DATE()
);

CREATE OR REPLACE TABLE dim_time_periods (
    period_id NUMBER PRIMARY KEY,
    period_name VARCHAR(50),
    start_date DATE,
    end_date DATE,
    quarter VARCHAR(10),
    year NUMBER,
    created_date DATE DEFAULT CURRENT_DATE()
);

-- Fact Tables (Mart Layer - references both schemas)
CREATE OR REPLACE TABLE fact_library_transactions (
    transaction_id NUMBER PRIMARY KEY,
    member_id NUMBER,
    book_id NUMBER,
    branch_id NUMBER,
    transaction_type VARCHAR(20), -- 'LOAN', 'RETURN', 'RESERVE', 'RENEWAL'
    transaction_date DATE,
    amount NUMBER(10,2),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    -- References: member_id -> src_investments_bolt.dim_members
    -- References: book_id -> src_investments_bolt.dim_books
    -- References: branch_id -> src_investments_bolt.dim_library_branches
);

CREATE OR REPLACE TABLE fact_member_activity (
    activity_id NUMBER PRIMARY KEY,
    member_id NUMBER,
    branch_id NUMBER,
    activity_date DATE,
    books_borrowed NUMBER,
    books_returned NUMBER,
    total_fines NUMBER(10,2),
    active_days NUMBER,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    -- References: member_id -> src_investments_bolt.dim_members
    -- References: branch_id -> src_investments_bolt.dim_library_branches
);

-- Views (Mart Layer - aggregate from facts and dims)
CREATE OR REPLACE VIEW vw_library_summary AS
SELECT 
    b.branch_name,
    COUNT(DISTINCT l.member_id) as total_members,
    COUNT(DISTINCT l.book_id) as unique_books_loaned,
    COUNT(l.loan_id) as total_loans,
    SUM(l.fine_amount) as total_fines,
    AVG(DATEDIFF(day, l.loan_date, COALESCE(l.return_date, CURRENT_DATE()))) as avg_loan_duration
FROM src_investments_bolt.dim_library_branches b
LEFT JOIN src_investments_bolt.fact_book_loans l ON b.branch_id = l.branch_id
GROUP BY b.branch_id, b.branch_name;

CREATE OR REPLACE VIEW vw_popular_books AS
SELECT 
    bk.book_id,
    bk.title,
    bk.author,
    COUNT(DISTINCT l.member_id) as borrower_count,
    COUNT(l.loan_id) as loan_count,
    AVG(l.fine_amount) as avg_fine
FROM src_investments_bolt.dim_books bk
LEFT JOIN src_investments_bolt.fact_book_loans l ON bk.book_id = l.book_id
GROUP BY bk.book_id, bk.title, bk.author
HAVING COUNT(l.loan_id) > 0
ORDER BY loan_count DESC;

CREATE OR REPLACE VIEW vw_member_activity_summary AS
SELECT 
    m.member_id,
    m.member_name,
    m.membership_type,
    COUNT(DISTINCT ma.activity_date) as active_days,
    SUM(ma.books_borrowed) as total_books_borrowed,
    SUM(ma.total_fines) as total_fines_paid
FROM src_investments_bolt.dim_members m
LEFT JOIN mart_investments_bolt.fact_member_activity ma ON m.member_id = ma.member_id
GROUP BY m.member_id, m.member_name, m.membership_type;

-- ============================================
-- POPULATE TEST DATA
-- ============================================

USE SCHEMA src_investments_bolt;

-- Insert Library Branches
INSERT INTO dim_library_branches VALUES
(1, 'Central Library', '123 Main St', 'New York', 'NY', '10001', '212-555-0101', CURRENT_DATE()),
(2, 'Downtown Branch', '456 Park Ave', 'New York', 'NY', '10002', '212-555-0102', CURRENT_DATE()),
(3, 'Uptown Branch', '789 Broadway', 'New York', 'NY', '10003', '212-555-0103', CURRENT_DATE());

-- Insert Books
INSERT INTO dim_books VALUES
(1, '978-0-123456-78-9', 'The Great Migration', 'John Smith', 'Tech Books', 2020, 'Technology', 'English', CURRENT_DATE()),
(2, '978-0-123456-79-6', 'Data Engineering Guide', 'Jane Doe', 'Tech Books', 2021, 'Technology', 'English', CURRENT_DATE()),
(3, '978-0-123456-80-2', 'Snowflake Deep Dive', 'Bob Johnson', 'Tech Books', 2022, 'Technology', 'English', CURRENT_DATE()),
(4, '978-0-123456-81-9', 'SQL Mastery', 'Alice Williams', 'Tech Books', 2023, 'Technology', 'English', CURRENT_DATE()),
(5, '978-0-123456-82-6', 'Cloud Architecture', 'Charlie Brown', 'Tech Books', 2023, 'Technology', 'English', CURRENT_DATE());

-- Insert Members
INSERT INTO dim_members VALUES
(1, 'Alice Johnson', 'alice@email.com', '555-1001', '100 First St', 'PREMIUM', '2023-01-15', CURRENT_DATE()),
(2, 'Bob Smith', 'bob@email.com', '555-1002', '200 Second St', 'STANDARD', '2023-02-20', CURRENT_DATE()),
(3, 'Charlie Brown', 'charlie@email.com', '555-1003', '300 Third St', 'PREMIUM', '2023-03-10', CURRENT_DATE()),
(4, 'Diana Prince', 'diana@email.com', '555-1004', '400 Fourth St', 'STANDARD', '2023-04-05', CURRENT_DATE()),
(5, 'Eve Wilson', 'eve@email.com', '555-1005', '500 Fifth St', 'PREMIUM', '2023-05-12', CURRENT_DATE());

-- Insert Authors
INSERT INTO dim_authors VALUES
(1, 'John Smith', 'USA', 1980, CURRENT_DATE()),
(2, 'Jane Doe', 'USA', 1985, CURRENT_DATE()),
(3, 'Bob Johnson', 'UK', 1975, CURRENT_DATE()),
(4, 'Alice Williams', 'Canada', 1990, CURRENT_DATE()),
(5, 'Charlie Brown', 'USA', 1988, CURRENT_DATE());

-- Insert Book Inventory
INSERT INTO fact_book_inventory VALUES
(1, 1, 1, 10, 7, 1, CURRENT_TIMESTAMP()),
(2, 2, 1, 5, 3, 0, CURRENT_TIMESTAMP()),
(3, 3, 2, 8, 5, 2, CURRENT_TIMESTAMP()),
(4, 4, 2, 6, 4, 1, CURRENT_TIMESTAMP()),
(5, 5, 3, 12, 9, 0, CURRENT_TIMESTAMP());

-- Insert Book Loans
INSERT INTO fact_book_loans VALUES
(1, 1, 1, 1, '2024-01-15', '2024-02-15', '2024-02-10', 0.00, 'RETURNED', CURRENT_TIMESTAMP()),
(2, 1, 2, 1, '2024-02-20', '2024-03-20', NULL, 0.00, 'ACTIVE', CURRENT_TIMESTAMP()),
(3, 2, 3, 2, '2024-03-01', '2024-04-01', '2024-04-05', 2.00, 'RETURNED', CURRENT_TIMESTAMP()),
(4, 3, 4, 2, '2024-03-10', '2024-04-10', NULL, 0.00, 'ACTIVE', CURRENT_TIMESTAMP()),
(5, 4, 5, 3, '2024-03-15', '2024-04-15', NULL, 0.00, 'ACTIVE', CURRENT_TIMESTAMP()),
(6, 5, 1, 1, '2024-03-20', '2024-04-20', NULL, 0.00, 'ACTIVE', CURRENT_TIMESTAMP());

USE SCHEMA mart_investments_bolt;

-- Insert Book Categories
INSERT INTO dim_book_categories VALUES
(1, 'Technology', NULL, 'Technology and Computing', CURRENT_DATE()),
(2, 'Science', NULL, 'Science and Research', CURRENT_DATE()),
(3, 'Fiction', NULL, 'Fictional Literature', CURRENT_DATE());

-- Insert Time Periods
INSERT INTO dim_time_periods VALUES
(1, 'Q1 2024', '2024-01-01', '2024-03-31', 'Q1', 2024, CURRENT_DATE()),
(2, 'Q2 2024', '2024-04-01', '2024-06-30', 'Q2', 2024, CURRENT_DATE()),
(3, 'Q3 2024', '2024-07-01', '2024-09-30', 'Q3', 2024, CURRENT_DATE());

-- Insert Library Transactions
INSERT INTO fact_library_transactions VALUES
(1, 1, 1, 1, 'LOAN', '2024-01-15', 0.00, CURRENT_TIMESTAMP()),
(2, 1, 1, 1, 'RETURN', '2024-02-10', 0.00, CURRENT_TIMESTAMP()),
(3, 1, 2, 1, 'LOAN', '2024-02-20', 0.00, CURRENT_TIMESTAMP()),
(4, 2, 3, 2, 'LOAN', '2024-03-01', 0.00, CURRENT_TIMESTAMP()),
(5, 2, 3, 2, 'RETURN', '2024-04-05', 2.00, CURRENT_TIMESTAMP()),
(6, 3, 4, 2, 'LOAN', '2024-03-10', 0.00, CURRENT_TIMESTAMP()),
(7, 4, 5, 3, 'LOAN', '2024-03-15', 0.00, CURRENT_TIMESTAMP()),
(8, 5, 1, 1, 'LOAN', '2024-03-20', 0.00, CURRENT_TIMESTAMP());

-- Insert Member Activity
INSERT INTO fact_member_activity VALUES
(1, 1, 1, '2024-01-15', 1, 0, 0.00, 1, CURRENT_TIMESTAMP()),
(2, 1, 1, '2024-02-10', 0, 1, 0.00, 1, CURRENT_TIMESTAMP()),
(3, 1, 1, '2024-02-20', 1, 0, 0.00, 1, CURRENT_TIMESTAMP()),
(4, 2, 2, '2024-03-01', 1, 0, 0.00, 1, CURRENT_TIMESTAMP()),
(5, 2, 2, '2024-04-05', 0, 1, 2.00, 1, CURRENT_TIMESTAMP()),
(6, 3, 2, '2024-03-10', 1, 0, 0.00, 1, CURRENT_TIMESTAMP()),
(7, 4, 3, '2024-03-15', 1, 0, 0.00, 1, CURRENT_TIMESTAMP()),
(8, 5, 1, '2024-03-20', 1, 0, 0.00, 1, CURRENT_TIMESTAMP());

-- ============================================
-- VALIDATION QUERIES
-- ============================================

SELECT '=== TABLE COUNTS ===' as validation;
SELECT 'src_investments_bolt.dim_library_branches' as table_name, COUNT(*) as row_count FROM src_investments_bolt.dim_library_branches
UNION ALL
SELECT 'src_investments_bolt.dim_books', COUNT(*) FROM src_investments_bolt.dim_books
UNION ALL
SELECT 'src_investments_bolt.dim_members', COUNT(*) FROM src_investments_bolt.dim_members
UNION ALL
SELECT 'src_investments_bolt.dim_authors', COUNT(*) FROM src_investments_bolt.dim_authors
UNION ALL
SELECT 'src_investments_bolt.fact_book_inventory', COUNT(*) FROM src_investments_bolt.fact_book_inventory
UNION ALL
SELECT 'src_investments_bolt.fact_book_loans', COUNT(*) FROM src_investments_bolt.fact_book_loans
UNION ALL
SELECT 'mart_investments_bolt.dim_book_categories', COUNT(*) FROM mart_investments_bolt.dim_book_categories
UNION ALL
SELECT 'mart_investments_bolt.dim_time_periods', COUNT(*) FROM mart_investments_bolt.dim_time_periods
UNION ALL
SELECT 'mart_investments_bolt.fact_library_transactions', COUNT(*) FROM mart_investments_bolt.fact_library_transactions
UNION ALL
SELECT 'mart_investments_bolt.fact_member_activity', COUNT(*) FROM mart_investments_bolt.fact_member_activity;

SELECT '=== VIEW VALIDATION ===' as validation;
SELECT COUNT(*) as vw_library_summary_rows FROM mart_investments_bolt.vw_library_summary;
SELECT COUNT(*) as vw_popular_books_rows FROM mart_investments_bolt.vw_popular_books;
SELECT COUNT(*) as vw_member_activity_summary_rows FROM mart_investments_bolt.vw_member_activity_summary;

SELECT '=== CROSS-SCHEMA DEPENDENCIES CHECK ===' as validation;
SELECT 
    'fact_library_transactions -> dim_members' as dependency,
    COUNT(*) as matching_rows
FROM mart_investments_bolt.fact_library_transactions ft
INNER JOIN src_investments_bolt.dim_members m ON ft.member_id = m.member_id;

SELECT '=== DATASET CREATION COMPLETE ===' as status;

