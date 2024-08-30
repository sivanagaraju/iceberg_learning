-- Create Customers table
CREATE TABLE hadoop_catalog.ecommerce.customers (
  customer_id INT,
  name STRING,
  email STRING,
  address STRING
) USING iceberg;

-- Create Orders table
CREATE TABLE hadoop_catalog.ecommerce.orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  status STRING
) USING iceberg
PARTITIONED BY (order_date);

-- Create Products table
CREATE TABLE hadoop_catalog.ecommerce.products (
  product_id INT,
  name STRING,
  price FLOAT,
  stock_quantity INT
) USING iceberg;

-- Create Order_Items table
CREATE TABLE hadoop_catalog.ecommerce.order_items (
  order_id INT,
  product_id INT,
  quantity INT,
  unit_price FLOAT
) USING iceberg;

-- Insert data into Customers table
INSERT INTO hadoop_catalog.ecommerce.customers
VALUES (1, 'John Doe', 'john@example.com', '123 Main St'),
       (2, 'Jane Smith', 'jane@example.com', '456 Elm St');

-- Insert data into Products table
INSERT INTO hadoop_catalog.ecommerce.products
VALUES (1, 'Widget A', 9.99, 100),
       (2, 'Gadget B', 19.99, 50);

-- Insert data into Orders table
INSERT INTO hadoop_catalog.ecommerce.orders
VALUES (1, 1, '2023-05-01', 'Completed'),
       (2, 2, '2023-05-02', 'Processing');

-- Insert data into Order_Items table
INSERT INTO hadoop_catalog.ecommerce.order_items
VALUES (1, 1, 2, 9.99),
       (1, 2, 1, 19.99),
       (2, 1, 3, 9.99);

-- Alter table: Add a column to Customers table
ALTER TABLE hadoop_catalog.ecommerce.customers
ADD COLUMN phone_number STRING;

-- Alter table: Rename a column in Products table
ALTER TABLE hadoop_catalog.ecommerce.products
RENAME COLUMN price TO unit_price;

-- Drop a table
DROP TABLE IF EXISTS hadoop_catalog.ecommerce.temp_table;

-- Upsert (merge) example for Products table
MERGE INTO hadoop_catalog.ecommerce.products t
USING (VALUES 
  (1, 'Widget A Updated', 10.99, 90),
  (3, 'New Product C', 29.99, 75)
) s(product_id, name, unit_price, stock_quantity)
ON t.product_id = s.product_id
WHEN MATCHED THEN
  UPDATE SET 
    t.name = s.name,
    t.unit_price = s.unit_price,
    t.stock_quantity = s.stock_quantity
WHEN NOT MATCHED THEN
  INSERT (product_id, name, unit_price, stock_quantity)
  VALUES (s.product_id, s.name, s.unit_price, s.stock_quantity);

-- Insert with a SELECT statement
INSERT INTO hadoop_catalog.ecommerce.order_items
SELECT o.order_id, p.product_id, 1, p.unit_price
FROM hadoop_catalog.ecommerce.orders o
CROSS JOIN hadoop_catalog.ecommerce.products p
WHERE o.order_id = 2 AND p.product_id = 2;

-- Perform maintenance tasks
CALL hadoop_catalog.system.rewrite_data_files('ecommerce.customers');
CALL hadoop_catalog.system.remove_orphan_files('ecommerce.orders');
CALL hadoop_catalog.system.expire_snapshots('ecommerce.products', TIMESTAMP '2023-04-01 00:00:00.000');
