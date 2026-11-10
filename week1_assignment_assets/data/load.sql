-- DDL + load for dialect: sqlserver

CREATE TABLE dim_category (
  category_id BIGINT,
  parent_category_id BIGINT,
  category_name NVARCHAR(255)
);
BULK INSERT dim_category FROM 'C:\\cb_data\\dim_category.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

CREATE TABLE dim_employee (
  employee_id BIGINT,
  employee_name NVARCHAR(255),
  manager_id BIGINT,
  role NVARCHAR(255),
  region NVARCHAR(255)
);
BULK INSERT dim_employee FROM 'C:\\cb_data\\dim_employee.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

CREATE TABLE dim_customer (
  customer_id BIGINT,
  customer_name NVARCHAR(255),
  city NVARCHAR(255),
  segment NVARCHAR(255),
  signup_date DATE
);
BULK INSERT dim_customer FROM 'C:\\cb_data\\dim_customer.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

CREATE TABLE dim_product (
  product_id BIGINT,
  product_name NVARCHAR(255),
  category_id BIGINT,
  unit_price DECIMAL(18,2),
  unit_cost DECIMAL(18,2),
  launch_date DATE
);
BULK INSERT dim_product FROM 'C:\\cb_data\\dim_product.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

CREATE TABLE fact_orders (
  order_id BIGINT,
  order_date DATE,
  customer_id BIGINT,
  sales_rep_id BIGINT,
  order_status NVARCHAR(255),
  order_total DECIMAL(18,2)
);
BULK INSERT fact_orders FROM 'C:\\cb_data\\fact_orders.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

CREATE TABLE fact_order_items (
  order_item_id BIGINT,
  order_id BIGINT,
  product_id BIGINT,
  quantity BIGINT,
  unit_price DECIMAL(18,2),
  line_amount DECIMAL(18,2)
);
BULK INSERT fact_order_items FROM 'C:\\cb_data\\fact_order_items.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

CREATE TABLE stg_orders_incr (
  order_id BIGINT,
  order_date DATE,
  customer_id BIGINT,
  sales_rep_id BIGINT,
  order_status NVARCHAR(255),
  order_total DECIMAL(18,2)
);
BULK INSERT stg_orders_incr FROM 'C:\\cb_data\\stg_orders_incr.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);

CREATE TABLE cdc_product_changes (
  change_id BIGINT,
  product_id BIGINT,
  operation NVARCHAR(255),
  change_ts NVARCHAR(255),
  product_name NVARCHAR(255),
  category_id BIGINT,
  unit_price DECIMAL(18,2),
  unit_cost DECIMAL(18,2),
  launch_date DATE
);
BULK INSERT cdc_product_changes FROM 'C:\\cb_data\\cdc_product_changes.csv' WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', TABLOCK);
