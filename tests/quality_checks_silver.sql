/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys
  - Unwanted spaces in string fields.
  - Data standardization and consistency
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- Data Standardization & Consistency
SELECT DISTINCT
  gen
FROM silver.erp_cust_az12;

-- ========================================
-- Checking 'silver.erp_loc_a101'
-- ========================================
-- Data Standardization & Consistency
SELECT DISTINCT
  cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ========================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ========================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
  *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
  OR subcat != TRIM(subcat)
  OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT
  maintenance
FROM silver.erp_px_cat_g1v2;



-- Check for unwanted Spaces
-- Expectation: No Results
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)


--Check Data consistency
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info
