CREATE or replace SCHEMA LA_POC_SILVER;

create or replace TABLE POC_DB.LA_POC_SILVER.BRAND_ORDER_TOTAL (
	BRAND_ID NUMBER(38,0),
	TOTAL NUMBER(38,0)
);

create or replace TABLE POC_DB.LA_POC_SILVER.ORDER_CATEGORY_BRAND_PRODUCT (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_ID NUMBER(38,0),
	BRAND_ID NUMBER(38,0),
	PRODUCT_ID NUMBER(38,0),
	YEAR NUMBER(38,0),
	MONTH NUMBER(38,0),
	TOTAL_ORDER NUMBER(38,0),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9),
	primary key (ID)
);

create or replace TABLE POC_DB.LA_POC_SILVER.ORDER_STATE (
	ID NUMBER(38,0) NOT NULL,
	STATE VARCHAR(16777216),
	YEAR NUMBER(38,0),
	MONTH NUMBER(38,0),
	TOTAL_ORDER NUMBER(38,0),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9),
	primary key (ID)
);

create or replace TABLE POC_DB.LA_POC_SILVER.PRODUCT_ORDER (
	PRODUCT_ORDER_ID NUMBER(38,0) NOT NULL,
	PRODUCT_ID VARCHAR(16777216),
	YEAR NUMBER(38,0),
	MONTH NUMBER(38,0),
	TOTAL_ORDER NUMBER(38,0),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9),
	primary key (PRODUCT_ORDER_ID)
);


CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_SILVER.GET_ORDER_PRODUCT()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    result = {};   
    var sel_query_product_order = "SELECT P.SUB_CATEGORY_ID,P.BRAND_ID,P.PRODUCT_ID,EXTRACT(YEAR FROM O.ORDER_DATE) AS YEAR,EXTRACT(MONTH FROM O.ORDER_DATE) AS MONTH,SUM(OP.QUANTITY) AS TOTAL_ORDER FROM POC_DB.LA_POC_RAW.PRODUCT P JOIN POC_DB.LA_POC_RAW.ORDER_PRODUCT OP ON P.PRODUCT_ID = OP.PRODUCT_ID JOIN POC_DB.LA_POC_RAW."ORDER" O ON O.ORDER_ID = OP.ORDER_ID GROUP BY P.SUB_CATEGORY_ID, P.BRAND_ID, P.PRODUCT_ID, EXTRACT(YEAR FROM O.ORDER_DATE), EXTRACT(MONTH FROM O.ORDER_DATE);";
    
    var table_name = "POC_DB.LA_POC_SILVER.ORDER_CATEGORY_BRAND_PRODUCT";
    var primary_key = "product_order_id";
    
   var callStmt = "call POC_DB.LA_POC_SILVER.GENERAL_INSERT_DB(?, ?, ?)";
   var stmt = snowflake.createStatement({ sqlText: callStmt, binds: [sel_query_product_order, table_name, primary_key] });
    stmt.execute(); 
    
    result.status = "SUCCESS";
} catch(err) {        
    result.message = err.message;
    result.status = "FAILED";
}
return result;
';


CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_SILVER.GENERAL_INSERT_DB("SELECT_QUERY" VARCHAR(16777216), "TABLE_NAME" VARCHAR(16777216), "PRIMARY_KEY" VARCHAR(16777216))
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



CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_SILVER.GET_TOTAL_BRAND_ORDER()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
   // Delete data from table la_poc_silver.brand_order_total
   var del_query = "delete from  la_poc_silver.brand_order_total;";
   var del_statement = snowflake.createStatement( {sqlText: del_query} );
   del_statement.execute(); 

   // Get total count grouping by brand_id  
   //var sel_query= "select brand_id,count(*) from POC_DB.poc_stg.order_product group by brand_id;";
   //var sel_stmt = snowflake.createStatement( {sqlText: sel_query} );
   //var sel_result_set = sel_stmt.execute();

   
    var sel_query = "SELECT p.product_id,EXTRACT(YEAR FROM o.order_date) AS year,EXTRACT(MONTH FROM o.order_date) AS month,sum(op.quantity) as count FROM
    POC_DB.POC_STG.product p
    JOIN
    POC_DB.POC_STG.order_product op ON p.product_id = op.product_id
    JOIN
    POC_DB.POC_STG.orders o ON op.order_id = o.order_id
    GROUP BY
    p.product_id,
    year,
    month
    ORDER BY
    p.product_id;"
    var sel_stmt = snowflake.createStatement( {sqlText: sel_query} );
    var sel_result_set = sel_stmt.execute();

   // Insert into la_poc_silver.brand_order_total
  // while (sel_result_set.next())   
  // {
     
  //	var brandid=sel_result_set.getColumnValue(1);
      //  var tot_cnt=sel_result_set.getColumnValue(2);
       // var temp_query = "insert into la_poc_silver.brand_order_total (brand_id,total) values (" ;
       // var insert_query= temp_query  + brandid +`,` + tot_cnt + `);`
       // var insert_stmt = snowflake.createStatement( {sqlText: insert_query} );
      //  var insert_result_set = insert_stmt.execute();  
   // }   
  sel_result_set;    
  return "Sucess";
   
';



CREATE OR REPLACE PROCEDURE POC_DB.LA_POC_SILVER.SF_SILVER_GOLD_TRANSFORM_DATA()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    result = {};       
    var sel_query_product_order = "SELECT TB.CATEGORY_ID,TB.YEAR,TB.MONTH,SUM(TOTAL_ORDER)AS TOTAL_ORDER FROM POC_DB.LA_POC_SILVER.ORDER_CATEGORY_BRAND_PRODUCT TB GROUP BY TB.CATEGORY_ID,TB.YEAR,TB.MONTH";
    
    
   var table_name = "POC_DB.LA_POC_GOLD.CATEGORY_ORDER";
    var primary_key = "id";
    
  var callStmt = "call POC_DB.LA_POC_SILVER.GENERAL_INSERT_DB(?, ?, ?)";
  var stmt = snowflake.createStatement({ sqlText: callStmt, binds: [sel_query_product_order, table_name, primary_key] });
    stmt.execute(); 

    var sel_query_order_brand = "SELECT TB.BRAND_ID,TB.YEAR,TB.MONTH,SUM(TOTAL_ORDER) AS TOTAL_ORDER FROM POC_DB.LA_POC_SILVER.ORDER_CATEGORY_BRAND_PRODUCT TB GROUP BY TB.BRAND_ID,TB.YEAR,TB.MONTH;";

    var table_name_brand = "POC_DB.LA_POC_GOLD.BRAND_ORDER";
    var callStmt_brand = "call POC_DB.LA_POC_SILVER.GENERAL_INSERT_DB(?, ?, ?)";
   var stmt_brand = snowflake.createStatement({ sqlText: callStmt_brand, binds: [sel_query_order_brand, table_name_brand, primary_key] });
    stmt_brand.execute();  
    result.status = "SUCCESS";
} catch(err) {        
    result.message = err.message;
    result.status = "FAILED";
}
return result;
';