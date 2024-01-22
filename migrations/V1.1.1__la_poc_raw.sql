CREATE or replace SCHEMA LA_POC_RAW;

create or replace TABLE ADDRESS (
	ADDRESS_ID NUMBER(38,0) PRIMARY KEY,
	ADDRESS_TYPE VARCHAR(25),
	ADDRESS VARCHAR(100),
	IS_VALID NUMBER(38,0),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9),
	STATE VARCHAR(50)
);

create or replace TABLE BRAND (
	BRAND_ID NUMBER(38,0) PRIMARY KEY,
	BRAND_DESCRIPTION VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE CATEGORY (
	CATEGORY_ID NUMBER(38,0) PRIMARY KEY,
	CATEGORY_NAME VARCHAR(25),
	PARENT_CATEGORY NUMBER(38,0),
	CATEGORY_DESCRIPTION VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE CONTACT (
	CONTACT_ID NUMBER(38,0) PRIMARY KEY,
	CONTACT_TYPE VARCHAR(25),
	CONTACT_VALUE VARCHAR(25),
	IS_VALID NUMBER(38,0),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE CUSTOMER (
	CUSTOMER_ID NUMBER(38,0)PRIMARY KEY,
	CUSTOMER_NAME VARCHAR(25),
	EMAIL_CONTACT_ID NUMBER(38,0),
	PHONE_CONTACT_ID NUMBER(38,0),
	CUSTOMER_ADDRESS_ID NUMBER(38,0),
	JOINED_DATE DATE,
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE ORDERS (
	ORDER_ID NUMBER(38,0) PRIMARY KEY,
	CUSTOMER_ID NUMBER(38,0),
	ORDER_DATE DATE,
	SHIPPING_ID NUMBER(38,0),
	SHIPPING_DATE DATE,
	SHIPPING_ADDRESS_ID NUMBER(38,0),
	BILLING_ADDRESS_ID NUMBER(38,0),
	TOTAL_AMOUNT FLOAT,
	DISCOUNTS NUMBER(38,0),
	TAX_APPLIED FLOAT,
	PAYMENT_ID NUMBER(38,0),
	PAYMENT_STATUS_ID NUMBER(38,0),
	SHIPPING_STATUS_ID NUMBER(38,0),
	ORDER_STATUS_ID NUMBER(38,0),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE ORDER_PRODUCT (
	ORDER_PRODUCT_ID NUMBER(10,0) PRIMARY KEY,
	ORDER_ID NUMBER(10,0),
	PRODUCT_ID NUMBER(10,0),
	QUANTITY NUMBER(10,0),
	UNIT_PRICE NUMBER(10,0),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE ORDER_STATUS (
	ORDER_STATUS_ID NUMBER(38,0) PRIMARY KEY,
	ORDER_STATUS VARCHAR(25),
	DESCRIPTION VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE PAYMENT_METHOD (
	PAYMENT_ID NUMBER(38,0)PRIMARY KEY,
	PAYMENT_METHOD VARCHAR(25),
	DESCRIPTION VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE PAYMENT_STATUS (
	PAYMENT_STATUS_ID NUMBER(38,0) PRIMARY KEY,
	PAYMENT_STATUS VARCHAR(25),
	DESCRIPTION VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE PRICE (
	PRICE_ID NUMBER(38,0) PRIMARY KEY,
	PRODUCT_ID NUMBER(38,0) NOT NULL,
	ORIGINAL_PRICE NUMBER(38,0) NOT NULL,
	CURRENT_SELLING_PRICE NUMBER(38,0) NOT NULL,
	CURRENT_DISCOUNT NUMBER(38,0) NOT NULL,
	DISCOUNT_APPLIED_DATE VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE PRODUCT (
	PRODUCT_ID NUMBER(38,0) COMMENT 'The PRODUCT_ID column in the PRODUCT table represents identifer with a particular record.',
	PRODUCT_NAME VARCHAR(100) COMMENT 'The PRODUCT_NAME column in the PRODUCT table represents the name of a product.',
	PRODUCT_DESCRIPTION VARCHAR(100) COMMENT 'The PRODUCT_DESCRIPTION column in the PRODUCT table represents the description of a product.',
	BRAND_ID NUMBER(38,0) COMMENT 'The BRAND_ID column in the PRODUCT table represents the identifier of a brand.',
	SUB_CATEGORY_ID NUMBER(38,0) COMMENT 'The SUB_CATEGORY_ID column in the PRODUCT table represents the sub category identifier of a product.',
	PRICE_ID NUMBER(38,0) COMMENT 'The PRICE_ID column in the PRODUCT table represents the price identifier of a product.',
	UPC NUMBER(38,0) COMMENT 'The UPC column in the PRODUCT table represents the universal product code of a product.',
	SKU_ID VARCHAR(100) COMMENT 'The SKU_ID represents the storage keeping unit identifier of the product',
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9) COMMENT 'The CREATED_TIMESTAMP represents the date the product was added in the product table ',
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9) COMMENT 'The UPDATED_TIMESTAMP represents the date the product was updated in the product table '
);

create or replace TABLE SHIPPING (
	SHIPPING_ID NUMBER(38,0) PRIMARY KEY,
	SHIPPING_METHOD_NAME VARCHAR(25),
	DESCRIPTION VARCHAR(100),
	EXPECTED_DELIVERY_TIME NUMBER(38,0) NOT NULL,
	COST NUMBER(38,0) NOT NULL,
	CARRIER_ID NUMBER(38,0) NOT NULL,
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE SHIPPING_CARRIER (
	CARRIER_ID NUMBER(38,0) PRIMARY KEY,
	CARRIER_NAME VARCHAR(25),
	DESCRIPTION VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE SHIPPING_STATUS (
	SHIPPING_STATUS_ID NUMBER(38,0) PRIMARY KEY,
	SHIPPING_STATUS VARCHAR(25),
	DESCRIPTION VARCHAR(100),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_RAW.ADLS_STG("TRANS_TABLE" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "DOMAIN_NAME" VARCHAR(16777216), "SOURCE_SCHEMA" VARCHAR(16777216), "TABLE_NAME" VARCHAR(16777216), "EXTERNAL_STG" VARCHAR(16777216), "TARGET_STG_SCHEMA" VARCHAR(16777216), "LOADED_TS" VARCHAR(16777216), "QUERY_TEXT" VARCHAR(16777216), "STG_LOAD_DATE" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try{
        var result = {};
        var query_text = QUERY_TEXT.replace(/FROM STAGE_FILE_NAME/gi, ``);
        result.query_text = query_text
        
        var trans_table_name= DB_NAME+`.`+TARGET_STG_SCHEMA+`.`+TRANS_TABLE;
        var transient_table = `CREATE TRANSIENT TABLE `+trans_table_name+` AS `+query_text ;
        
        var transient_table_stmt = snowflake.createStatement({sqlText:transient_table});
    	var transient_table_exec = transient_table_stmt.execute();
        
        var truncate_exec = snowflake.execute({sqlText:`TRUNCATE TABLE `+trans_table_name});
         
    
    	var file_date = LOADED_TS.slice(0,10).split(`-`).join(`/`);
    	var list_query = `list @`+DB_NAME+`.`+TARGET_STG_SCHEMA+`.`+EXTERNAL_STG+`/`+DOMAIN_NAME+`/`+TABLE_NAME+`/;`;
    	var list_query_stmt = snowflake.createStatement({sqlText:list_query});
    	var list_query_op = list_query_stmt.execute();
    	var list_query_qid = list_query_stmt.getQueryId();
        result.list_query = list_query
        result.list_query_qid = list_query_qid   

        var file_name_query = `SELECT $$''$$ || listagg(split("name", $$`+TABLE_NAME+`$$)[1], $$'',''$$) ||  $$''$$ from TABLE(RESULT_SCAN(''`+list_query_qid+`'')) 
    	where TO_TIMESTAMP_NTZ("last_modified",''DY, DD MON YYYY HH24:MI:SS GMT'') >= ''`+STG_LOAD_DATE+`'' and "name" like ''%.parquet'';`;
    	var file_name_query_exec = snowflake.execute({sqlText:file_name_query});
    	file_name_query_exec.next();
    	var file_names = file_name_query_exec.getColumnValue(1);

        result.file_names = file_names
        
    	var load_time = new Date();
        result.load_time = result.load_time=load_time.toISOString().replace(`T`,` `).replace(`Z`,``);
        
    	var file_loc = `@`+DB_NAME+`.`+TARGET_STG_SCHEMA+`.`+EXTERNAL_STG+`/`+DOMAIN_NAME+`/`+TABLE_NAME;
    	var select_query = QUERY_TEXT.replace(/STAGE_FILE_NAME/gi, file_loc);        
    	select_query = `( `+select_query+`) FILES = (`+file_names+`) FILE_FORMAT = ( TYPE = PARQUET );`;

        result.select_query = select_query
        
        var trans_table = DB_NAME+`.`+TARGET_STG_SCHEMA+`.`+TRANS_TABLE;
        result.trans_table = trans_table
        
        var copy_sql = `COPY INTO `+trans_table+` from `+select_query ;

        result.copy_sql = copy_sql
    
    	var copy_sql_stmt = snowflake.createStatement({sqlText:copy_sql});
    	var copy_sql_exec = copy_sql_stmt.execute();
        
    	
    }
    catch (err) {        
            result.message=err.message;
            result.status="FAILED"
        }
    
    return result;
';



CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_RAW.GENERAL_INSERT_DB("SELECT_QUERY" VARCHAR(16777216), "TABLE_NAME" VARCHAR(16777216), "PRIMARY_KEY" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try{
    var truncateQuery = "TRUNCATE TABLE " + TABLE_NAME + ";";
    var truncateStmt = snowflake.createStatement({ sqlText: truncateQuery });
    truncateStmt.execute();
    result = {};     
    select_query_local=SELECT_QUERY; 
    var sel_stmt = snowflake.createStatement( {sqlText: select_query_local} );
    var sel_result_set = sel_stmt.execute();
    var column_count = sel_result_set.getColumnCount();
    result.column_count = column_count;     
    var query_columns =""; 
    for (var i = 1; i <= column_count; i++) {
        var col = sel_result_set.getColumnName(i);
        if(i===1)
            query_columns = query_columns+PRIMARY_KEY+","+col;
        else
            query_columns = query_columns+","+col;
        }
        query_columns = query_columns+","+"updated_timestamp";
    result.query_columns = query_columns;
    column_values="";
   
 while(sel_result_set.next())   
    {
       var random_id = Math.floor(Math.random() * 100000) + 1 
	   for(var i=1;i<=column_count;i++){
       result.date_type_name = typeof sel_result_set.getColumnValue(i);
       if (typeof sel_result_set.getColumnValue(i) === ''string''){
           if(i===1)
             column_values = column_values+random_id+`,`+`''`+sel_result_set.getColumnValue(i)+`''`;
           else
              column_values = column_values+`,`+`''`+sel_result_set.getColumnValue(i)+`''`;}
        else
        {
         if(i===1)
             column_values = column_values+random_id+`,`+sel_result_set.getColumnValue(i);
           else
              column_values = column_values+`,`+sel_result_set.getColumnValue(i);
        }
        }      
        
        result.query_columns = query_columns;
       var insert_query = `insert into `+TABLE_NAME+`(`+query_columns+`) VALUES`+`(`+column_values+`,CURRENT_TIMESTAMP);`;
       
        result.insert_query = insert_query;
        var insert_stmt = snowflake.createStatement( {sqlText: insert_query});
       var insert_result_set = insert_stmt.execute();  
         
       result.column_values = column_values;
	   result.insert_query = insert_query;
   column_values="";
  }
  }
   
  catch(err) {        
            result.message=err.message;
            result.status="FAILED"
        }
return result
';


CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_RAW.LOAD_TRANS_TO_RAW("DOMAIN_NAME" VARCHAR(16777216), "SOURCE_DATABASE_NAME" VARCHAR(16777216), "SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE_NAME" VARCHAR(16777216), "TARGET_DATABASE_NAME" VARCHAR(16777216), "TARGET_TABLE_NAME" VARCHAR(16777216), "TARGET_STG_SCHEMA" VARCHAR(16777216), "TARGET_ODS_SCHEMA" VARCHAR(16777216), "LOAD_TS" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var step_number                     =  1;
    var stepstart_description           = ''PROCEDURE START'';
    var stepstart_status                = ''SUCCESS - START'';
    var steplast_description            = ''PROCEDURE END'';
    var steplast_status                 = ''SUCCESS - END'';
    var source_table_database           = SOURCE_DATABASE_NAME;
    var source_table_name               = SOURCE_TABLE_NAME;
    var target_table_name1              = TARGET_TABLE_NAME
    var target_table_name               = `"` + TARGET_TABLE_NAME + `"`;
    var target_table_database           = TARGET_DATABASE_NAME;
    var source_table_schema             = TARGET_STG_SCHEMA;
    var target_table_schema             = TARGET_ODS_SCHEMA;
    var proc_name                       = ''LOAD_DATA FROM STG TO ODS'';
    var domain_name                     = DOMAIN_NAME;
    var source_schema                   = SOURCE_SCHEMA;
    var load_ts                         = LOAD_TS;
    var load_time                       = new Date();
    var result                          = {};           
    try{
        var cmd1 = "SELECT listagg(COLUMN_NAME,'','') within group (order by ORDINAL_POSITION) COL_LIST,listagg(''c.''||COLUMN_NAME||''=''||''e.''||column_name,'','') within group (order by ORDINAL_POSITION) upd_list,listagg(''e.''||COLUMN_NAME,'','') within group (order by ORDINAL_POSITION)  values_clause FROM INFORMATION_SCHEMA.COLUMNS WHERE upper(TABLE_SCHEMA)= UPPER(:1) and upper(table_name) = UPPER(:2)  and upper(table_catalog) = UPPER(:3)  ORDER BY ORDINAL_POSITION ";  
        var col_stmt_exec = snowflake.execute({ sqlText: cmd1,binds:[target_table_schema,target_table_name1,target_table_database]});
        col_stmt_exec.next();
        var col_list=col_stmt_exec.getColumnValue(1);
        var upd_list=col_stmt_exec.getColumnValue(2);
        var values_clause=col_stmt_exec.getColumnValue(3);
    
        var cmd_desc = `describe table `+target_table_database+`.`+target_table_schema+`.`+ target_table_name + `;`;
        var stmt_desc_sql = snowflake.createStatement({ sqlText: cmd_desc});
        var stmt_desc = stmt_desc_sql.execute();
        var stmt_desc_qid = stmt_desc_sql.getQueryId();
        stmt_desc.next();
        var cmd_join = `select listagg(join_cond,'' and '') from ( select ''UPPER(C.''||$1||'') = UPPER(E.''||$1||'')'' JOIN_COND   from table(result_scan(''` + stmt_desc_qid +`'')) );`
        var stmt_join = snowflake.execute({ sqlText: cmd_join});
        stmt_join.next();  
        var join_cond=stmt_join.getColumnValue(1);                                
        var merge_stmt_sql = `MERGE into `+target_table_database+`.`+target_table_schema+`.` + target_table_name + ` c using ( select distinct * from `+source_table_database+`.`+source_table_schema+`.` + source_table_name+` ) e
        on ` +join_cond +` when matched  then
            update set ` + upd_list + `
        when not matched then
            Insert (` + col_list + `) values (` + values_clause + `);`;                            
    
        var merge_stmt = snowflake.createStatement({ sqlText: merge_stmt_sql});
        var merge_stmt_exec = merge_stmt.execute();
        var merge_stmt_qid = merge_stmt.getQueryId();

        var merge_rslt_sql = `SELECT * FROM TABLE(RESULT_SCAN(''`+merge_stmt_qid+`''))`;
        var merge_rslt = snowflake.execute({sqlText:merge_rslt_sql});
        merge_rslt.next();
        var merge_rows_inserted = merge_rslt.getColumnValue(1);
        var merge_rows_updated = merge_rslt.getColumnValue(2);
        var total_rows_affected = merge_rows_inserted+merge_rows_updated;

    	  result.row_loaded=total_rows_affected;
    	  result.load_time=load_time.toISOString().replace(`T`,` `).replace(`Z`,``);
        result.message="SUCCESS";

        
    }
    catch (err) {
        result.status = "Failed";
	  result.message=err.message;
        }

        return result;
';



CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_RAW.SF_RAW_SILVER_TRANSFORM_DATA()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    result = {};       
    var sel_query_product_order = "SELECT P.SUB_CATEGORY_ID AS CATEGORY_ID,P.BRAND_ID,P.PRODUCT_ID,EXTRACT(YEAR FROM O.ORDER_DATE) AS YEAR,EXTRACT(MONTH FROM O.ORDER_DATE) AS MONTH,SUM(OP.QUANTITY) AS TOTAL_ORDER FROM POC_DB.LA_POC_RAW.PRODUCT P JOIN POC_DB.LA_POC_RAW.ORDER_PRODUCT OP ON P.PRODUCT_ID = OP.PRODUCT_ID JOIN POC_DB.LA_POC_RAW.ORDERS O ON O.ORDER_ID = OP.ORDER_ID GROUP BY P.SUB_CATEGORY_ID, P.BRAND_ID, P.PRODUCT_ID, EXTRACT(YEAR FROM O.ORDER_DATE), EXTRACT(MONTH FROM O.ORDER_DATE);";
    
    
   var table_name = "POC_DB.LA_POC_SILVER.ORDER_CATEGORY_BRAND_PRODUCT";
    var primary_key = "id";
    
  var callStmt = "call POC_DB.LA_POC_SILVER.GENERAL_INSERT_DB(?, ?, ?)";
  var stmt = snowflake.createStatement({ sqlText: callStmt, binds: [sel_query_product_order, table_name, primary_key] });
    stmt.execute(); 

    var sel_query_order_state = "SELECT A.STATE,EXTRACT(YEAR FROM O.ORDER_DATE) AS year,EXTRACT(MONTH FROM O.ORDER_DATE) AS MONTH,COUNT(O.ORDER_ID) AS TOTAL_ORDER FROM POC_DB.LA_POC_RAW.ORDERS O JOIN POC_DB.LA_POC_RAW.ADDRESS A ON A.ADDRESS_ID = O.SHIPPING_ADDRESS_ID GROUP BY A.STATE, YEAR, MONTH;";

    var table_name_state = "POC_DB.LA_POC_SILVER.ORDER_STATE";
    var callStmt_state = "call POC_DB.LA_POC_SILVER.GENERAL_INSERT_DB(?, ?, ?)";
   var stmt_state = snowflake.createStatement({ sqlText: callStmt_state, binds: [sel_query_order_state, table_name_state, primary_key] });
    stmt_state.execute();  
    result.status = "SUCCESS";
} catch(err) {        
    result.message = err.message;
    result.status = "FAILED";
}
return result;
';