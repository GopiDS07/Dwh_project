-- Make sure you are using the DWH database
USE dwh;

-- ================= BRONZE TABLES =================
--- CRM TABLES---

DROP TABLE IF EXISTS dwh.bronze_crm_cust_info;
CREATE TABLE dwh.bronze_crm_cust_info (
    cst_id             VARCHAR(50),
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS dwh.bronze_crm_prd_info;
CREATE TABLE dwh.bronze_crm_prd_info (
    prd_id        INT,
    prd_key       VARCHAR(50),
    prd_num       VARCHAR(50),
    prd_cost      INT,
    prd_line      VARCHAR(10),
    prd_start_dt  DATETIME,
    prd_end_dt    DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS dwh.bronze_crm_sales_details_info;
CREATE TABLE dwh.bronze_crm_sales_details_info (
    sls_ord_num   VARCHAR(50),
    sls_prd_key   VARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--- ERP Tables---

DROP TABLE IF EXISTS dwh.bronze_erp_cust_az12;
CREATE TABLE dwh.bronze_erp_cust_az12 (
    cid   VARCHAR(50),
    bdate DATE,
    gen   VARCHAR(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS dwh.bronze_erp_loc_a101_info;
CREATE TABLE dwh.bronze_erp_loc_a101_info (
    cid  VARCHAR(50),
    ctry VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS dwh.bronze_erp_px_cat_gv12_info;
CREATE TABLE dwh.bronze_erp_px_cat_gv12_info (
    id          VARCHAR(50),
    cat         VARCHAR(50),
    subcat      VARCHAR(50),
    maintenance VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



         
         
