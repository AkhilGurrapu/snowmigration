# Snowflake Migration Execution Guide
## Complete Dependency Discovery & Migration: IMCUST → IMSDLC

**CRITICAL UPDATE**: This migration includes **ALL downstream and upstream dependencies**, not just the 8 listed objects.

---

## 🎯 What This Migration Does

### Starting Objects (Base)
1. **SRC_INVESTMENTS_BOLT.stock_metadata_raw** (table)
2. **MART_INVESTMENTS_BOLT.dim_stocks** (table)
3. **MART_INVESTMENTS_BOLT.dim_portfolios** (table)
4. **MART_INVESTMENTS_BOLT.fact_transactions** (table)
5. **MART_INVESTMENTS_BOLT.fact_daily_positions** (table)
6. **MART_INVESTMENTS_BOLT.vw_current_holdings** (view)
7. **MART_INVESTMENTS_BOLT.sp_load_dim_stocks** (procedure)
8. **MART_INVESTMENTS_BOLT.sp_calculate_daily_positions** (procedure)

### Complete Discovery Includes
- ✅ **Upstream Dependencies**: Objects that base objects reference
- ✅ **Downstream Dependencies**: Objects that reference base objects
- ✅ **Transitive Dependencies**: Multi-level dependency chains
- ✅ **Runtime Dependencies**: Objects used by procedures at runtime
- ✅ **View Dependencies**: Nested view references
- ✅ **Complete Dependency Graph**: Using recursive SQL CTEs

### How Discovery Works

```sql
-- Recursive CTE discovers ALL dependencies:
WITH RECURSIVE upstream_deps AS (
    -- Find what base objects depend on
    SELECT ... FROM object_dependencies
    WHERE referencing_object IN (base_objects)

    UNION ALL

    -- Find what THOSE depend on (recursive)
    SELECT ... FROM object_dependencies
    JOIN upstream_deps ON ...
)
```

This ensures **NOTHING is missed**!

---

## 📋 Execution Phases

### Phase 1: Complete Discovery (IMCUST)
**Script**: `IMCUST/01_discovery_complete.sql`

**What It Does**:
1. Starts with 8 base objects
2. Recursively finds ALL upstream dependencies (what they depend on)
3. Recursively finds ALL downstream dependencies (what depends on them)
4. Creates complete migration object list with priority ordering
5. Identifies external dependencies (outside migration scope)
6. Generates metadata for all discovered objects

**Expected Output**:
- Complete list of ALL objects to migrate (could be 8, could be 50+!)
- Dependency graph with migration order
- Row counts for validation
- External dependency warnings

**Run This First**: This determines the FULL scope!

---

### Phase 2: Enhanced Data Share (IMCUST + IMSDLC)

**Scripts**:
- `IMCUST/03_create_share_complete.sql` - Create share for ALL tables
- `IMSDLC/01_consume_share_complete.sql` - Consume share in target

**What Changes**:
- Share creation dynamically includes ALL discovered tables
- Not limited to original 5 tables
- Includes upstream AND downstream table dependencies

---

### Phase 3: Complete DDL Extraction (IMCUST)

**Script**: `IMCUST/04_extract_ddl_complete.sql`

**What It Does**:
- Extracts DDL for ALL objects discovered in Phase 1
- Handles tables, views, procedures, functions
- Generates transformation script for prod_db → dev_db
- Preserves clustering keys, constraints, comments

---

### Phase 4: Object Creation in Dependency Order (IMSDLC)

**Script**: `IMSDLC/02_create_all_objects.sql`

**Critical**: Objects created in correct order:
1. Upstream tables (dependencies of base)
2. Base tables
3. Base views
4. Downstream tables (depend on base)
5. Downstream views
6. All procedures (last, as they may reference everything)

---

### Phase 5: Complete Data Migration (IMSDLC)

**Script**: `IMSDLC/05_populate_all_data.sql`

**What It Does**:
- Migrates data for ALL tables (not just 5)
- Uses INSERT INTO ... SELECT FROM shared database
- Maintains dependency order
- Tracks row counts for each table

---

### Phase 6: Comprehensive Validation (IMSDLC)

**Script**: `IMSDLC/06_validate_all_objects.sql`

**Validates**:
- Row counts match for ALL tables
- ALL views are queryable
- ALL procedures executable
- ALL dependencies resolved
- No broken references

---

### Phase 7: Final Verification (IMSDLC)

**Script**: `IMSDLC/08_verify_all_dependencies.sql`

**Confirms**:
- Complete dependency graph in target
- No external dependencies pointing to source
- All runtime dependencies present
- Migration 100% complete

---

## 🔍 Why Complete Dependency Discovery Matters

### Example Scenario

**Without Complete Discovery**:
```
You migrate: vw_current_holdings
It depends on: fact_daily_positions ✅ (you migrated)
But fact_daily_positions uses: sp_calc_position (procedure) ❌ (missed!)
And sp_calc_position reads: ref_market_data (table) ❌ (missed!)
Result: View works, but procedures fail at runtime!
```

**With Complete Discovery**:
```
1. Base: vw_current_holdings
2. Upstream: fact_daily_positions (direct dependency)
3. Downstream: sp_calc_position (references the view)
4. Upstream of sp_calc_position: ref_market_data (runtime dependency)
5. ALL migrated ✅
Result: Everything works!
```

---

## 🚨 Critical Differences from Original Plan

| Aspect | Original Plan | Enhanced Plan |
|--------|--------------|---------------|
| Objects | 8 fixed objects | 8 base + ALL dependencies |
| Discovery | Manual list | Recursive SQL discovery |
| Tables | 5 specific tables | All tables in dependency tree |
| Views | 1 specific view | All views in dependency tree |
| Procedures | 2 specific procedures | All procedures that reference objects |
| Validation | Check 8 objects | Check ALL discovered objects |
| Risk | **HIGH** (missing deps) | **LOW** (complete coverage) |

---

## 📊 Discovery Output Example

After running Phase 1, you might see:

```
MIGRATION SUMMARY
================================================
Base Objects: 8
├── Tables: 5
├── Views: 1
└── Procedures: 2

Upstream Dependencies Discovered: 12
├── Tables: 7 (referenced by base objects)
├── Views: 3 (referenced in view definitions)
└── Functions: 2 (used in procedures)

Downstream Dependencies Discovered: 15
├── Views: 8 (depend on base tables)
├── Procedures: 5 (reference base objects)
└── Dynamic Tables: 2 (sourced from base)

TOTAL OBJECTS TO MIGRATE: 35
================================================
```

**This is why complete discovery is critical!**

---

## 🛡️ Safety Features

### 1. External Dependency Detection
```sql
-- Warns if any object references something outside migration scope
SELECT * FROM external_dependencies_warning;
-- Example:
-- vw_current_holdings → PROD_DB.EXTERNAL_SCHEMA.ref_data ⚠️
-- Action: Include in migration or update reference
```

### 2. Circular Dependency Detection
```sql
-- Recursive CTE includes loop prevention
WHERE level < 10  -- Stops infinite loops
```

### 3. Migration Priority Ordering
```sql
-- Objects automatically sequenced:
-- Priority 0: Upstream (migrate FIRST)
-- Priority 1: Base (migrate SECOND)
-- Priority 2: Downstream (migrate LAST)
```

---

## ✅ Pre-Execution Checklist

Before starting migration:

- [ ] PAT tokens configured (`.env.imcust_pat`, `.env.imsdlc_pat`)
- [ ] ACCOUNTADMIN access to both accounts
- [ ] ADMIN_WH warehouse available in both accounts
- [ ] DEV_DB and schemas exist in IMSDLC
- [ ] ACCOUNT_USAGE schema accessible (for dependency queries)
- [ ] At least 3-hour wait after object creation (ACCOUNT_USAGE latency)

---

## 🎯 Success Criteria (Enhanced)

| Criterion | Validation |
|-----------|------------|
| ✅ All base objects migrated | Query IMSDLC: 8 base objects exist |
| ✅ All upstream deps migrated | No missing references in views/procedures |
| ✅ All downstream deps migrated | All dependent objects functional |
| ✅ Row counts match (all tables) | Source count = Target count for EVERY table |
| ✅ No broken dependencies | OBJECT_DEPENDENCIES shows complete graph |
| ✅ All views queryable | SELECT * FROM each view succeeds |
| ✅ All procedures executable | CALL each procedure succeeds |
| ✅ No external dependencies | No references to PROD_DB remain |

---

## 🚀 Execution Commands

### Phase 1: Discovery (REQUIRED FIRST)
```bash
# Connect to IMCUST
# Run: IMCUST/01_discovery_complete.sql
# Review output: How many objects discovered?
# Save complete_migration_objects result
```

### Phase 2-7: Execute in sequence
```bash
# Each phase builds on previous
# Validate after EACH phase
# Do NOT proceed if validation fails
```

---

## 📞 What to Expect

**Best Case**: Discovery finds 8-15 objects (light dependencies)
**Realistic Case**: Discovery finds 20-40 objects (moderate dependencies)
**Complex Case**: Discovery finds 50+ objects (heavy dependencies)

**All cases handled automatically by recursive discovery!**

---

## 🔧 Troubleshooting

### "ACCOUNT_USAGE data is stale"
- **Issue**: Objects created <3 hours ago not in OBJECT_DEPENDENCIES
- **Solution**: Wait 3 hours or use INFORMATION_SCHEMA (less complete)

### "Circular dependency detected"
- **Issue**: Object A → Object B → Object A
- **Solution**: Review dependency path, may need manual intervention

### "External dependency found"
- **Issue**: Object references schema outside MART/SRC
- **Solution**: Either include in migration or update references

---

## Next Steps

1. **Review this guide completely**
2. **Execute Phase 1 discovery**
3. **Review discovered objects list**
4. **Confirm migration scope**
5. **Proceed with Phase 2-7**

**Ready to start with Phase 1 discovery!**
