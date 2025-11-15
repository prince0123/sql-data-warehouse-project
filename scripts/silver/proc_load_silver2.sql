
CREATE OR ALTER PROCEDURE silver.load_silver as 
BEGIN
    PRINT'>> Truncating Data into silver.crm_cust_info'

    TRUNCATE TABLE silver.crm_cust_info;
    PRINT'>> Inserting Data into silver.crm_cust_info'
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        t.cst_id,
        t.cst_key,
        COALESCE(TRIM(t.cst_firstname), 'N/A') AS cst_firstname, -- removed unwanted spaces and also took care of NULLs
        COALESCE(TRIM(t.cst_lastname), 'N/A') AS cst_lastname, -- removed unwanted spaces and also took care of NULLs
        CASE  
            WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single' -- removed unwanted spaces and also took care of NULLs
            WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'-- normalized marital_status values 
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE  
            WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'Female' ---- removed unwanted spaces and also took care of NULLs
            WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Male'-- normalized Gender column
            ELSE 'N/A'
        END AS cst_gndr,
        TRY_CONVERT(date, t.cst_create_date) AS cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id
                   ORDER BY TRY_CONVERT(datetime, cst_create_date) DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
    ) t
    WHERE t.flag_last = 1   -- select latest data
      AND TRY_CONVERT(date, t.cst_create_date) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM silver.crm_cust_info s
          WHERE s.cst_id = t.cst_id
      );


     PRINT'>> Truncating Data into silver.crm_prd_info'
     TRUNCATE TABLE silver.crm_prd_info;

    PRINT'>> Inserting Data into silver.crm_prd_info'


      insert into silver.crm_prd_info(
    prd_id,cat_id,

    prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

    select 
    prd_id,
    replace(SUBSTRING(prd_key,1,5) ,'-','_') as cat_id, -- derived columns
    substring(prd_key,7,len(prd_key)) as prd_key, -- derived columns
    prd_nm,
    isnull(prd_cost,0) as prd_cost, -- handling nulls
    case upper(trim(prd_line)) -- data normalization
            when  'M' then 'Mountain'
            when  'R' then 'Road'
            when  'S' then 'Other Sales'
            when  'T' then 'Touring'
        else 'N/A'
    end as prd_line,
    cast(prd_start_dt as date) as prd_start_dt, -- type casting
    cast(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date)  as prd_end_dt -- data type casting and data enrichment
    from
    bronze.crm_prd_info

    PRINT'>> Truncating Data into silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT'>> Inserting Data into silver.crm_sales_details';


    insert into silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,sls_cust_id,
    sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
    select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    case
        when sls_sales <=0 or sls_sales!= abs(sls_quantity*sls_price) or sls_sales is null -- handling missing data 
        then abs(sls_quantity*sls_price)
        else sls_sales
    end as sls_sales,
    sls_quantity,
    case 
        when sls_price is null or sls_price<=0        -- handling invalid/missing
        then sls_sales/nullif(sls_quantity,0)
        else sls_price
    end as sls_price
    from bronze.crm_sales_details


    PRINT'>> Truncating Data into silver.crm_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT'>> Inserting Data into silver.crm_cust_az12';


    insert into silver.erp_cust_az12(cid,bdate, gen)

    select 
    case

     when CID Like 'NAS%'then SUBSTRING(cid,4,len(cid)) -- handled invalid values
     else CID
     end as cid ,
     case
         when BDATE> GETDATE() then NULL   -- handled invalid values
         else BDATE
    end as bdate,
    case
        when upper(trim(GEN )) in ('M','Male') then'Male' -- data normalization and missing values
        when upper(trim(GEN )) in ('F','Female')then'Female'
        else 'N/A'
    end as GEN

    from bronze.erp_cust_az12


    PRINT'>> Truncating Data into silver.crm_loc_a101';
    TRUNCATE TABLE  silver.erp_loc_a101;
    PRINT'>> Inserting Data into silver.crm_loc_a101';


    insert into silver.erp_loc_a101(
    cid,cntry)
    select 
    replace(CID,'-','') as cid,
    case                                                            -- Normalize or handled mising data
        when trim(cntry )in ('DE','Germany') then 'Germany'
        when trim(cntry) in ('USA','US','United States') then 'United States'
        when trim(cntry) is null or trim(cntry)='' then 'N/A'
        else cntry
    end as cntry
    from
    bronze.erp_loc_a101


    PRINT'>> Truncating Data into silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT'>> Inserting Data into silver.erp_px_cat_g1v2'


      insert into silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance)
    select
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
    from bronze.erp_px_cat_g1v2;
END
