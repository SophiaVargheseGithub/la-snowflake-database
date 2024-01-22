CREATE or replace SCHEMA LA_POC_GOLD;

create or replace TABLE POC_DB.LA_POC_GOLD.BRAND_ORDER (
	ID NUMBER(38,0) NOT NULL,
	BRAND_ID NUMBER(38,0),
	YEAR NUMBER(38,0),
	MONTH NUMBER(38,0),
	TOTAL_ORDER NUMBER(38,0),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9),
	primary key (ID)
);

create or replace TABLE POC_DB.LA_POC_GOLD.CATEGORY_ORDER (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_ID NUMBER(38,0),
	YEAR NUMBER(38,0),
	MONTH NUMBER(38,0),
	TOTAL_ORDER NUMBER(38,0),
	UPDATED_TIMESTAMP TIMESTAMP_NTZ(9),
	primary key (ID)
);