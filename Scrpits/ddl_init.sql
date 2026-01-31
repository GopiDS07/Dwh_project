DROP TABLE IF EXISTS bronze.crm_cust_info;

create table bronze.crm_cust_info(
         cst_id varchar(50),
         cst_key varchar(50),
         cst_firstname varchar(50),
         cst_lastname varchar(50),
         cst_marital_status varchar(50),
		 cst_gndr varchar(50),
		 cst_create_date date  ) ;

DROP TABLE IF EXISTS bronze.crm_prd_info;         
create table bronze.crm_prd_info(
         prd_id int,
         prd_key varchar(50),
         prd_num varchar(50),
         prd_cost int,
         prd_line varchar(10),
         prd_start_dt datetime,
         prd_end_dt datetime ) ;
         
DROP TABLE IF EXISTS bronze.crm_sales_details_info; 
         
create table bronze.crm_sales_details_info(
         sls_ord_num varchar(50),
         sls_prd_key varchar(50),
         sls_cust_id int,
         sls_order_dt int,
         sls_ship_dt int,
		 sls_due_dt int,
		 sls_sales int,
         sls_quantity int,
         sls_price int) ;

DROP TABLE IF EXISTS bronze.erp_cust_az12; 
create table bronze.erp_cust_az12(
		cid varchar(50),
         bdate date,
         gen varchar(10));
         
DROP TABLE IF EXISTS bronze.erp_loc_a101_info;          
create table bronze.erp_loc_a101_info(
          cid varchar(50),
          ctry varchar(50)  );
          
DROP TABLE IF EXISTS bronze.erp_px_cat_gv12_info;         
create table bronze.erp_px_cat_gv12_info(
      id varchar(50),
      cat varchar(50),
      subcat varchar(50),
      maintenance varchar(50)
) ;    
