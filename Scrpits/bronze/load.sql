-- ================= CRM CUSTOMER =================

TRUNCATE TABLE dwh.bronze_crm_cust_info;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_crm/cust_info.csv'
INTO TABLE dwh.bronze_crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@cst_id, cst_key, cst_firstname, cst_lastname,
 cst_marital_status, cst_gndr, @cst_create_date)
SET
    cst_id = NULLIF(TRIM(@cst_id), ''),
    cst_create_date =
        CASE
            WHEN TRIM(@cst_create_date) IN ('', '\r', '\n') THEN NULL
            ELSE STR_TO_DATE(TRIM(@cst_create_date), '%Y-%m-%d')
        END;


-- ================= CRM PRODUCT =================

TRUNCATE TABLE dwh.bronze_crm_prd_info;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_crm/prd_info.csv'
INTO TABLE dwh.bronze_crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    prd_id,
    prd_key,
    prd_num,
    @prd_cost,
    prd_line,
    @prd_start_dt,
    @prd_end_dt
)
SET
    prd_cost = NULLIF(TRIM(@prd_cost), ''),
    prd_start_dt =
        CASE
            WHEN TRIM(@prd_start_dt) = '' THEN NULL
            ELSE STR_TO_DATE(TRIM(@prd_start_dt), '%Y-%m-%d')
        END,
    prd_end_dt =
        CASE
            WHEN TRIM(@prd_end_dt) = '' THEN NULL
            ELSE STR_TO_DATE(TRIM(@prd_end_dt), '%Y-%m-%d')
        END;


-- ================= CRM SALES =================

TRUNCATE TABLE dwh.bronze_crm_sales_details_info;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_crm/sales_details.csv'
INTO TABLE dwh.bronze_crm_sales_details_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    sls_ord_num,
    sls_prd_key,
    @sls_cust_id,
    @sls_order_dt,
    @sls_ship_dt,
    @sls_due_dt,
    @sls_sales,
    @sls_quantity,
    @sls_price
)
SET
    sls_cust_id   = NULLIF(TRIM(@sls_cust_id), ''),
    sls_order_dt = NULLIF(TRIM(@sls_order_dt), ''),
    sls_ship_dt  = NULLIF(TRIM(@sls_ship_dt), ''),
    sls_due_dt   = NULLIF(TRIM(@sls_due_dt), ''),
    sls_sales    = NULLIF(TRIM(@sls_sales), ''),
    sls_quantity = NULLIF(TRIM(@sls_quantity), ''),
    sls_price    = NULLIF(TRIM(@sls_price), '');

-- ================= ERP =================

TRUNCATE TABLE dwh.bronze_erp_cust_az12;
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_erp/CUST_AZ12.csv'
INTO TABLE dwh.bronze_erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE dwh.bronze_erp_loc_a101_info;
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_erp/LOC_A101.csv'
INTO TABLE dwh.bronze_erp_loc_a101_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

TRUNCATE TABLE dwh.bronze_erp_px_cat_gv12_info;
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_erp/PX_CAT_G1V2.csv'
INTO TABLE dwh.bronze_erp_px_cat_gv12_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
