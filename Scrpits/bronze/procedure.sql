DELIMITER $$

DROP PROCEDURE IF EXISTS bronze.load_bronze$$

CREATE PROCEDURE bronze.load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    /* Error handler */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT '==========================================' AS msg;
        SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS msg;
        SELECT '==========================================' AS msg;
    END;

    SET batch_start_time = NOW();

    SELECT '================================================' AS msg;
    SELECT 'Loading Bronze Layer' AS msg;
    SELECT '================================================' AS msg;

    /* ================= CRM TABLES ================= */

    SELECT '------------------------------------------------' AS msg;
    SELECT 'Loading CRM Tables' AS msg;
    SELECT '------------------------------------------------' AS msg;

    /* ---------- crm_cust_info ---------- */
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_cust_info' AS msg;
    TRUNCATE TABLE bronze.crm_cust_info;

    SELECT '>> Inserting Data Into: bronze.crm_cust_info' AS msg;
    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_crm/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
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

    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ',
           TIMESTAMPDIFF(SECOND, start_time, end_time),
           ' seconds') AS msg;

    /* ---------- crm_prd_info ---------- */
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_prd_info' AS msg;
    TRUNCATE TABLE bronze.crm_prd_info;

    SELECT '>> Inserting Data Into: bronze.crm_prd_info' AS msg;
    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
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

    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ',
           TIMESTAMPDIFF(SECOND, start_time, end_time),
           ' seconds') AS msg;

    /* ---------- crm_sales_details_info ---------- */
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.crm_sales_details_info' AS msg;
    TRUNCATE TABLE bronze.crm_sales_details_info;

    SELECT '>> Inserting Data Into: bronze.crm_sales_details_info' AS msg;
    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details_info
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

    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ',
           TIMESTAMPDIFF(SECOND, start_time, end_time),
           ' seconds') AS msg;

    /* ================= ERP TABLES ================= */

    SELECT '------------------------------------------------' AS msg;
    SELECT 'Loading ERP Tables' AS msg;
    SELECT '------------------------------------------------' AS msg;

    /* ---------- erp_cust_az12 ---------- */
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_cust_az12' AS msg;
    TRUNCATE TABLE bronze.erp_cust_az12;

    SELECT '>> Inserting Data Into: bronze.erp_cust_az12' AS msg;
    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_erp/CUST_AZ12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ',
           TIMESTAMPDIFF(SECOND, start_time, end_time),
           ' seconds') AS msg;

    /* ---------- erp_loc_a101 ---------- */
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_loc_a101_info' AS msg;
    TRUNCATE TABLE bronze.erp_loc_a101_info;

    SELECT '>> Inserting Data Into: bronze.erp_loc_a101_info' AS msg;
    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_erp/LOC_A101.csv'
    INTO TABLE bronze.erp_loc_a101_info
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ',
           TIMESTAMPDIFF(SECOND, start_time, end_time),
           ' seconds') AS msg;

    /* ---------- erp_px_cat_g1v2 ---------- */
    SET start_time = NOW();
    SELECT '>> Truncating Table: bronze.erp_px_cat_gv12_info' AS msg;
    TRUNCATE TABLE bronze.erp_px_cat_gv12_info;

    SELECT '>> Inserting Data Into: bronze.erp_px_cat_gv12_info' AS msg;
    LOAD DATA INFILE
    'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/source_erp/PX_CAT_G1V2.csv'
    INTO TABLE bronze.erp_px_cat_gv12_info
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ',
           TIMESTAMPDIFF(SECOND, start_time, end_time),
           ' seconds') AS msg;

    SET batch_end_time = NOW();

    SELECT '==========================================' AS msg;
    SELECT 'Loading Bronze Layer is Completed' AS msg;
    SELECT CONCAT('Total Load Duration: ',
           TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time),
           ' seconds') AS msg;
    SELECT '==========================================' AS msg;

END$$

DELIMITER ;
