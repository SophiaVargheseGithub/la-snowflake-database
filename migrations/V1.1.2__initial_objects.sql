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
