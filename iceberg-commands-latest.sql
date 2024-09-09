-- Create Customers table with supported constraints and default values
-- Create Customers table with supported constraints
CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.customers (
  customer_id INT NOT NULL,
  name STRING NOT NULL,
  email STRING NOT NULL,
  address STRING,
  phone_number STRING,
  registration_date DATE,
  last_login_date TIMESTAMP,
  is_active BOOLEAN,
  loyalty_points INT
) USING iceberg;

-- Create Orders table with supported constraints
CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.orders (
  order_id INT NOT NULL,
  customer_id INT NOT NULL,
  order_date DATE NOT NULL,
  status STRING NOT NULL,
  total_amount DECIMAL(10,2) NOT NULL,
  last_updated TIMESTAMP
) USING iceberg
PARTITIONED BY (order_date);

-- Create Products table with supported constraints
CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.products (
  product_id INT NOT NULL,
  name STRING NOT NULL,
  description STRING,
  price DECIMAL(10,2) NOT NULL,
  stock_quantity INT NOT NULL,
  category STRING,
  created_at TIMESTAMP,
  is_available BOOLEAN
) USING iceberg;

-- Create Order_Items table with supported constraints
CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.order_items (
  order_item_id INT NOT NULL,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(10,2) NOT NULL,
  subtotal DECIMAL(10,2) NOT NULL
) USING iceberg;

-- Add a comment to a table (metadata)
ALTER TABLE `calm-hub-qa`.ecommerce.customers 
SET TBLPROPERTIES ('comment' = 'This table stores customer information');

-- Add a comment to a column (metadata)
ALTER TABLE `calm-hub-qa`.ecommerce.products 
ALTER COLUMN price COMMENT 'The current price of the product';

-- Set write order for a table
ALTER TABLE `calm-hub-qa`.ecommerce.orders 
SET TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd'
);

-- Specify sort order for a table
ALTER TABLE `calm-hub-qa`.ecommerce.order_items
SET TBLPROPERTIES (
  'sort-order' = 'order_id ASC NULLS LAST'
);
-- Create a view for order summaries (views don't support constraints, but we'll include NOT NULL for clarity)
CREATE OR REPLACE VIEW `calm-hub-qa`.ecommerce.order_summaries AS
SELECT 
  o.order_id,
  c.name AS customer_name NOT NULL,
  o.order_date NOT NULL,
  o.status NOT NULL,
  o.total_amount NOT NULL,
  COUNT(oi.order_item_id) AS item_count NOT NULL
FROM `calm-hub-qa`.ecommerce.orders o
JOIN `calm-hub-qa`.ecommerce.customers c ON o.customer_id = c.customer_id
JOIN `calm-hub-qa`.ecommerce.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, c.name, o.order_date, o.status, o.total_amount;

-- Create a temporary table for high-value orders
CREATE TEMPORARY TABLE `calm-hub-qa`.ecommerce.high_value_orders
USING iceberg
AS SELECT * FROM `calm-hub-qa`.ecommerce.orders WHERE total_amount > 1000;

-- Note: Temporary tables typically don't support adding constraints after creation,
-- so we're creating it as-is from the select statement.


-- Create Customers table
--CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.customers (
--  customer_id INT,
--  name STRING,
--  email STRING,
--  address STRING
--) USING iceberg;
--
---- Create Orders table
--CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.orders (
--  order_id INT,
--  customer_id INT,
--  order_date DATE,
--  status STRING
--) USING iceberg
--PARTITIONED BY (order_date);
--
---- Create Products table
--CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.products (
--  product_id INT,
--  name STRING,
--  price FLOAT,
--  stock_quantity INT
--) USING iceberg;
--
---- Create Order_Items table
--CREATE TABLE IF NOT EXISTS `calm-hub-qa`.ecommerce.order_items (
--  order_id INT,
--  product_id INT,
--  quantity INT,
--  unit_price FLOAT
--) USING iceberg;
--
---- Recreate the order_items table with a new column
--CREATE TABLE `calm-hub-qa`.ecommerce.order_items (
--  order_item_id INT,
--  order_id INT,
--  product_id INT,
--  quantity INT,
--  unit_price FLOAT,
--  subtotal FLOAT
--) USING iceberg;
--
---- Create a view for order summaries
--CREATE OR REPLACE VIEW `calm-hub-qa`.ecommerce.order_summaries AS
--SELECT 
--  o.order_id,
--  c.name AS customer_name,
--  o.order_date,
--  o.status,
--  o.total_amount,
--  COUNT(oi.order_item_id) AS item_count
--FROM `calm-hub-qa`.ecommerce.orders o
--JOIN `calm-hub-qa`.ecommerce.customers c ON o.customer_id = c.customer_id
--JOIN `calm-hub-qa`.ecommerce.order_items oi ON o.order_id = oi.order_id
--GROUP BY o.order_id, c.name, o.order_date, o.status, o.total_amount;
--
-- Create a temporary table for high-value orders
CREATE TEMPORARY TABLE `calm-hub-qa`.ecommerce.high_value_orders
USING iceberg
AS SELECT * FROM `calm-hub-qa`.ecommerce.orders WHERE total_amount > 50;

-- Data Insertion Statements
----------------------------

-- Insert initial data into Customers table
INSERT INTO `calm-hub-qa`.ecommerce.customers
SELECT * FROM (
  VALUES 
    (1, 'John Doe', 'john@example.com', '123 Main St'),
    (2, 'Jane Smith', 'jane@example.com', '456 Elm St')
) AS new_customers
WHERE NOT EXISTS (SELECT 1 FROM `calm-hub-qa`.ecommerce.customers LIMIT 1);

-- Insert initial data into Products table
INSERT INTO `calm-hub-qa`.ecommerce.products
SELECT * FROM (
  VALUES 
    (1, 'Widget A', 9.99, 100),
    (2, 'Gadget B', 19.99, 50)
) AS new_products
WHERE NOT EXISTS (SELECT 1 FROM `calm-hub-qa`.ecommerce.products LIMIT 1);

-- Insert initial data into Orders table
INSERT INTO `calm-hub-qa`.ecommerce.orders
SELECT * FROM (
  VALUES 
    (1, 1, to_date('2023-05-01', 'yyyy-MM-dd'), 'Completed'),
    (2, 2, to_date('2023-05-02', 'yyyy-MM-dd'), 'Processing')
) AS new_orders(order_id, customer_id, order_date, status)
WHERE NOT EXISTS (SELECT 1 FROM `calm-hub-qa`.ecommerce.orders LIMIT 1);

-- Insert initial data into Order_Items table
INSERT INTO `calm-hub-qa`.ecommerce.order_items
SELECT * FROM (
  VALUES 
    (1, 1, 2, 9.99),
    (1, 2, 1, 19.99),
    (2, 1, 3, 9.99)
) AS new_order_items
WHERE NOT EXISTS (SELECT 1 FROM `calm-hub-qa`.ecommerce.order_items LIMIT 1);

-- Insert more data into Customers table
INSERT INTO `calm-hub-qa`.ecommerce.customers (customer_id, name, email, address, phone_number)
VALUES 
  (3, 'Alice Johnson', 'alice@example.com', '789 Oak St', '555-1111'),
  (4, 'Bob Williams', 'bob@example.com', '101 Pine St', '555-2222'),
  (5, 'Carol Brown', 'carol@example.com', '202 Maple St', '555-3333'),
  (6, 'David Lee', 'david@example.com', '303 Birch St', '555-4444'),
  (7, 'Emma Davis', 'emma@example.com', '404 Cedar St', '555-5555'),
  (8, 'Frank Miller', 'frank@example.com', '505 Walnut St', '555-6666'),
  (9, 'Grace Wilson', 'grace@example.com', '606 Chestnut St', '555-7777'),
  (10, 'Henry Taylor', 'henry@example.com', '707 Spruce St', '555-8888'),
  (11, 'Luisa Rodriguez', 'luisa@example.es', 'Calle Mayor 123, Madrid, Spain', '+34 91 123 4567'),
  (12, 'Akira Tanaka', 'akira@example.jp', '1-2-3 Shibuya, Tokyo, Japan', '+81 3 1234 5678');

-- Insert more data into Products table
INSERT INTO `calm-hub-qa`.ecommerce.products (product_id, name, unit_price, stock_quantity)
VALUES 
  (4, 'Gizmo D', 14.99, 75),
  (5, 'Doohickey E', 24.99, 60),
  (6, 'Thingamajig F', 39.99, 40),
  (7, 'Whatchamacallit G', 49.99, 30),
  (8, 'Doodad H', 7.99, 200),
  (9, 'Contraption I', 59.99, 25),
  (10, 'Gizmo J', 34.99, 50),
  (11, 'Widget K', 12.99, 150);

-- Insert more data into Orders table
INSERT INTO `calm-hub-qa`.ecommerce.orders (order_id, customer_id, order_date, status)
VALUES 
  (3, 3, to_date('2023-05-03', 'yyyy-MM-dd'), 'Shipped'),
  (4, 4, to_date('2023-05-04', 'yyyy-MM-dd'), 'Processing'),
  (5, 5, to_date('2023-05-05', 'yyyy-MM-dd'), 'Completed'),
  (6, 6, to_date('2023-05-06', 'yyyy-MM-dd'), 'Processing'),
  (7, 7, to_date('2023-05-07', 'yyyy-MM-dd'), 'Shipped'),
  (8, 8, to_date('2023-05-08', 'yyyy-MM-dd'), 'Completed'),
  (9, 9, to_date('2023-05-09', 'yyyy-MM-dd'), 'Processing'),
  (10, 10, to_date('2023-05-10', 'yyyy-MM-dd'), 'Shipped'),
  (11, 1, to_date('2023-05-11', 'yyyy-MM-dd'), 'Processing'),
  (12, 11, to_date('2023-05-12', 'yyyy-MM-dd'), 'Processing'),
  (13, 12, to_date('2023-05-13', 'yyyy-MM-dd'), 'Shipped');

-- Insert more data into Order_Items table
INSERT INTO `calm-hub-qa`.ecommerce.order_items (order_item_id, order_id, product_id, quantity, unit_price, subtotal)
VALUES 
  (1, 1, 1, 2, 9.99, 19.98),
  (2, 1, 2, 1, 19.99, 19.99),
  (3, 2, 1, 3, 9.99, 29.97),
  (4, 3, 4, 2, 14.99, 29.98),
  (5, 3, 5, 1, 24.99, 24.99),
  (6, 4, 6, 1, 39.99, 39.99),
  (7, 5, 2, 2, 19.99, 39.98),
  (8, 5, 3, 1, 29.99, 29.99),
  (9, 6, 7, 1, 49.99, 49.99),
  (10, 6, 8, 3, 7.99, 23.97),
  (11, 7, 9, 1, 59.99, 59.99),
  (12, 7, 10, 2, 34.99, 69.98),
  (13, 8, 11, 4, 12.99, 51.96),
  (14, 8, 1, 1, 9.99, 9.99),
  (15, 9, 2, 2, 19.99, 39.98),
  (16, 9, 3, 1, 29.99, 29.99),
  (17, 10, 4, 3, 14.99, 44.97),
  (18, 10, 5, 1, 24.99, 24.99),
  (19, 11, 9, 3, 59.99, 179.97),
  (20, 11, 7, 2, 49.99, 99.98),
  (21, 11, 10, 4, 34.99, 139.96),
  (22, 12, 6, 2, 39.99, 79.98),
  (23, 12, 8, 5, 7.99, 39.95),
  (24, 13, 11, 3, 12.99, 38.97),
  (25, 13, 9, 1, 59.99, 59.99);

-- Table Alteration Statements
------------------------------

-- Add phone_number column to Customers table
ALTER TABLE `calm-hub-qa`.ecommerce.customers ADD COLUMN phone_number STRING;

-- Rename price column to unit_price in Products table
ALTER TABLE `calm-hub-qa`.ecommerce.products RENAME COLUMN price TO unit_price;

-- Add a new column to the orders table
ALTER TABLE `calm-hub-qa`.ecommerce.orders ADD COLUMN total_amount FLOAT;

-- Data Manipulation Statements
-------------------------------

-- Update the total_amount in the Orders table
UPDATE `calm-hub-qa`.ecommerce.orders o
SET total_amount = (
  SELECT SUM(subtotal)
  FROM `calm-hub-qa`.ecommerce.order_items oi
  WHERE oi.order_id = o.order_id
);

-- Update the stock quantity for a product
UPDATE `calm-hub-qa`.ecommerce.products
SET stock_quantity = stock_quantity - 5
WHERE product_id = 1;

-- Delete a customer (be careful with this in production!)
DELETE FROM `calm-hub-qa`.ecommerce.customers
WHERE customer_id = 2;

-- Perform a complex update using a CTE
WITH order_product_counts AS (
  SELECT order_id, COUNT(DISTINCT product_id) AS product_count
  FROM `calm-hub-qa`.ecommerce.order_items
  GROUP BY order_id
)
UPDATE `calm-hub-qa`.ecommerce.orders o
SET status = 'Priority Processing'
WHERE EXISTS (
  SELECT 1
  FROM order_product_counts opc
  WHERE o.order_id = opc.order_id
    AND opc.product_count > 2
)
AND o.status = 'Processing';

-- Upsert (merge) example for Products table
MERGE INTO `calm-hub-qa`.ecommerce.products t
USING (
  SELECT * FROM (VALUES 
    (1, 'Widget A Updated', 10.99, 90),
    (3, 'New Product C', 29.99, 75)
  ) AS s(product_id, name, unit_price, stock_quantity)
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN
  UPDATE SET 
    t.name = s.name,
    t.unit_price = s.unit_price,
    t.stock_quantity = s.stock_quantity
WHEN NOT MATCHED THEN
  INSERT (product_id, name, unit_price, stock_quantity)
  VALUES (s.product_id, s.name, s.unit_price, s.stock_quantity);

-- Query Statements
-------------------

-- Verify the data in Customers table
SELECT * FROM `calm-hub-qa`.ecommerce.customers;

-- Verify the data in Products table
SELECT * FROM `calm-hub-qa`.ecommerce.products;

-- Verify the data in Orders table
SELECT * FROM `calm-hub-qa`.ecommerce.orders;

-- Verify the data in Order_Items table
SELECT * FROM `calm-hub-qa`.ecommerce.order_items;

-- Verify the data in Order_Summaries view
SELECT * FROM `calm-hub-qa`.ecommerce.order_summaries;

-- Verify the data in High_Value_Orders temporary table
SELECT * FROM `calm-hub-qa`.ecommerce.high_value_orders;

-- Verify newly inserted customer data
SELECT * FROM `calm-hub-qa`.ecommerce.customers WHERE customer_id > 5;

-- Verify newly inserted product data
SELECT * FROM `calm-hub-qa`.ecommerce.products WHERE product_id > 6;

-- Verify newly inserted order data
SELECT * FROM `calm-hub-qa`.ecommerce.orders WHERE order_id > 5;

-- Verify newly inserted order item data
SELECT * FROM `calm-hub-qa`.ecommerce.order_items WHERE order_item_id > 8;

-- Verify newly inserted data in Order_Summaries view
SELECT * FROM `calm-hub-qa`.ecommerce.order_summaries WHERE order_id > 5;

-- Verify newly inserted data in High_Value_Orders temporary table
SELECT * FROM `calm-hub-qa`.ecommerce.high_value_orders WHERE order_id > 5;



-- Time Travel Queries
----------------------

-- Query data as of a specific point in time
SELECT * FROM `calm-hub-qa`.ecommerce.orders TIMESTAMP AS OF '2023-05-10 12:00:00';

-- Query data as of a specific snapshot ID
SELECT * FROM `calm-hub-qa`.ecommerce.orders VERSION AS OF 1234567890;

-- Compare data between two points in time
SELECT 
  o1.order_id,
  o1.status AS old_status,
  o2.status AS new_status
FROM `calm-hub-qa`.ecommerce.orders VERSION AS OF 1234567890 o1
JOIN `calm-hub-qa`.ecommerce.orders CURRENT o2
  ON o1.order_id = o2.order_id
WHERE o1.status != o2.status;

-- Schema Evolution
-------------------

-- Add a new column to the customers table
ALTER TABLE `calm-hub-qa`.ecommerce.customers
ADD COLUMN loyalty_points INT;

-- Rename a column in the products table
ALTER TABLE `calm-hub-qa`.ecommerce.products
RENAME COLUMN unit_price TO price;

-- Change the data type of a column in the orders table
ALTER TABLE `calm-hub-qa`.ecommerce.orders
ALTER COLUMN total_amount TYPE DECIMAL(10,2);

-- Add a column with a default value to the order_items table
ALTER TABLE `calm-hub-qa`.ecommerce.order_items
ADD COLUMN is_gift BOOLEAN DEFAULT false;

-- Partition Management
-----------------------

-- Add a new partition field to the orders table
ALTER TABLE `calm-hub-qa`.ecommerce.orders
ADD PARTITION FIELD months(order_date);

-- Rewrite data files to apply the new partitioning
CALL calm-hub-qa.system.rewrite_data_files(
  table => 'ecommerce.orders',
  strategy => 'binpack'
);

-- Remove a partition field from the orders table
ALTER TABLE `calm-hub-qa`.ecommerce.orders
DROP PARTITION FIELD months(order_date);

-- Data Compaction
------------------

-- Compact small files in the products table
CALL calm-hub-qa.system.rewrite_data_files(
  table => 'ecommerce.products',
  strategy => 'binpack',
  options => map('min-input-files','5', 'target-file-size-bytes','134217728')
);

-- Sort data in the order_items table
CALL calm-hub-qa.system.rewrite_data_files(
  table => 'ecommerce.order_items',
  strategy => 'sort',
  sort_order => 'order_id ASC NULLS LAST'
);

-- Metadata Management
----------------------

-- Expire old snapshots to clean up metadata
CALL calm-hub-qa.system.expire_snapshots(
  table => 'ecommerce.orders',
  older_than => TIMESTAMP '2023-04-01 00:00:00',
  retain_last => 10
);

-- Remove orphan files
CALL calm-hub-qa.system.remove_orphan_files(
  table => 'ecommerce.orders'
);

-- Advanced Queries
-------------------

-- Use metadata tables to analyze table history
SELECT
  h.made_current_at,
  s.operation,
  s.snapshot_id,
  s.summary['total-records'] AS total_records,
  s.summary['added-records'] AS added_records,
  s.summary['deleted-records'] AS deleted_records
FROM calm-hub-qa.ecommerce.orders.history h
JOIN calm-hub-qa.ecommerce.orders.snapshots s
  ON h.snapshot_id = s.snapshot_id
ORDER BY h.made_current_at DESC
LIMIT 10;

-- Query data at a specific version using CTE
WITH orders_v1 AS (
  SELECT * FROM `calm-hub-qa`.ecommerce.orders VERSION AS OF 1234567890
)
SELECT 
  customer_id,
  COUNT(*) as order_count,
  SUM(total_amount) as total_spent
FROM orders_v1
GROUP BY customer_id;

-- Incremental Processing
-------------------------

-- Identify new records since last processed snapshot
SET @last_processed_snapshot = 1234567890;

INSERT INTO `calm-hub-qa`.ecommerce.processed_orders
SELECT o.*
FROM `calm-hub-qa`.ecommerce.orders o
LEFT OUTER JOIN `calm-hub-qa`.ecommerce.orders VERSION AS OF @last_processed_snapshot p
  ON o.order_id = p.order_id
WHERE p.order_id IS NULL;

-- Update the last processed snapshot
SET @new_snapshot = SELECT snapshot_id FROM calm-hub-qa.ecommerce.orders.snapshots ORDER BY committed_at DESC LIMIT 1;

-- (Assume we have a table to store the last processed snapshot)
UPDATE `calm-hub-qa`.ecommerce.processing_metadata
SET last_processed_snapshot = @new_snapshot
WHERE table_name = 'orders';

-- Table Maintenance
--------------------

-- Analyze table to update statistics
CALL calm-hub-qa.system.rewrite_manifests(
  table => 'ecommerce.customers'
);


-- Data Migration
-----------------

-- Migrate data from a non-Iceberg table to an Iceberg table
CREATE TABLE `calm-hub-qa`.ecommerce.customer_reviews
USING iceberg
AS SELECT * FROM `calm-hub-qa`.ecommerce_old.customer_reviews;

-- Perform zero-copy migration (assuming the source is Parquet format)
ALTER TABLE `calm-hub-qa`.ecommerce.customer_reviews
SET TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);

-- Table Optimization
---------------------

-- Optimize the layout of data files
CALL calm-hub-qa.system.rewrite_data_files(
  table => 'ecommerce.orders',
  strategy => 'sort',
  sort_order => 'order_date DESC, customer_id ASC',
  options => map('target-file-size-bytes', '536870912')
);

-- Z-Order optimization for multi-dimensional querying
CALL calm-hub-qa.system.rewrite_data_files(
  table => 'ecommerce.order_items',
  strategy => 'zorder',
  zorder_columns => array('order_id', 'product_id')
);

-- Advanced Querying Techniques
-------------------------------

-- Use metadata filtering for efficient querying
SELECT *
FROM `calm-hub-qa`.ecommerce.orders
WHERE order_date > '2023-01-01'
  AND order_date IN (
    SELECT DISTINCT order_date
    FROM `calm-hub-qa`.ecommerce.orders.files
    WHERE lower_bounds['order_date']::date <= '2023-06-30'
      AND upper_bounds['order_date']::date >= '2023-01-01'
  );

-- Perform time-series analysis using window functions
SELECT
  order_date,
  SUM(total_amount) AS daily_total,
  AVG(SUM(total_amount)) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS 7_day_moving_avg
FROM `calm-hub-qa`.ecommerce.orders
WHERE order_date >= '2023-01-01'
GROUP BY order_date
ORDER BY order_date;

-- Use table properties for query optimization
ALTER TABLE `calm-hub-qa`.ecommerce.orders
SET TBLPROPERTIES (
  'read.split.target-size' = '268435456',
  'read.split.planning-lookback' = '2880000000'
);

-- Data Governance and Auditing
-------------------------------

-- Set table properties for data governance
ALTER TABLE `calm-hub-qa`.ecommerce.customers
SET TBLPROPERTIES (
  'owner' = 'data_team',
  'sensitive' = 'true',
  'pii' = 'true',
  'retention.period' = '1095 days'
);

-- Query audit history
SELECT
  h.made_current_at,
  s.operation,
  s.summary,
  p.user_id,
  p.properties
FROM `calm-hub-qa`.ecommerce.orders.history h
JOIN `calm-hub-qa`.ecommerce.orders.snapshots s
  ON h.snapshot_id = s.snapshot_id
LEFT JOIN `calm-hub-qa`.ecommerce.orders.properties p
  ON s.properties_id = p.properties_id
ORDER BY h.made_current_at DESC
LIMIT 10;

-- Data Validation and Quality Checks
-------------------------------------

-- Check for data anomalies
SELECT
  order_date,
  COUNT(*) as order_count,
  AVG(total_amount) as avg_order_amount,
  MIN(total_amount) as min_order_amount,
  MAX(total_amount) as max_order_amount
FROM `calm-hub-qa`.ecommerce.orders
GROUP BY order_date
HAVING COUNT(*) > 1000 OR AVG(total_amount) > 1000
ORDER BY order_date;

-- Validate referential integrity
SELECT o.order_id, o.customer_id
FROM `calm-hub-qa`.ecommerce.orders o
LEFT JOIN `calm-hub-qa`.ecommerce.customers c
  ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Advanced Partition Management
--------------------------------

-- Dynamically update partitioning based on data distribution
CREATE PROCEDURE update_orders_partitioning()
LANGUAGE PYTHON
AS $$
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Analyze current data distribution
df = spark.table("calm-hub-qa.ecommerce.orders")
total_orders = df.count()
date_distribution = df.groupBy("order_date").count().collect()

# Determine optimal partitioning
if total_orders > 1000000:
    spark.sql("ALTER TABLE `calm-hub-qa`.ecommerce.orders ADD PARTITION FIELD days(order_date)")
elif total_orders > 100000:
    spark.sql("ALTER TABLE `calm-hub-qa`.ecommerce.orders ADD PARTITION FIELD months(order_date)")
else:
    spark.sql("ALTER TABLE `calm-hub-qa`.ecommerce.orders ADD PARTITION FIELD years(order_date)")

# Rewrite data files to apply new partitioning
spark.sql("CALL calm-hub-qa.system.rewrite_data_files(table => 'ecommerce.orders', strategy => 'binpack')")
$$;

-- Call the procedure to update partitioning
CALL update_orders_partitioning();

-- Continuous Data Processing
-----------------------------

-- Set up a streaming query to process new orders in real-time
CREATE TEMPORARY VIEW new_orders_stream
USING iceberg
OPTIONS (
  'stream-from-timestamp' = '2023-06-01 00:00:00',
  'streaming-skip-delete-snapshots' = 'true'
)
AS SELECT * FROM `calm-hub-qa`.ecommerce.orders;

-- Process new orders (this would typically be part of a streaming job)
INSERT INTO `calm-hub-qa`.ecommerce.processed_orders
SELECT
  order_id,
  customer_id,
  order_date,
  total_amount,
  CASE
    WHEN total_amount > 1000 THEN 'High Value'
    WHEN total_amount > 500 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS order_category
FROM new_orders_stream;