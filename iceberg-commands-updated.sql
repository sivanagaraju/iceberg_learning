-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS `calm-hub-qa`.ecommercedb 
LOCATION 's3a://calm-hub-qa/ecommercedb/';

-- Use the newly created database
USE `calm-hub-qa`.ecommercedb;

-- Create Customers table if not exists
CREATE TABLE IF NOT EXISTS customers (
  customer_id INT,
  name STRING,
  email STRING,
  address STRING
) USING iceberg;

-- Create Orders table if not exists
CREATE TABLE IF NOT EXISTS orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  status STRING
) USING iceberg
PARTITIONED BY (order_date);

-- Create Products table if not exists
CREATE TABLE IF NOT EXISTS products (
  product_id INT,
  name STRING,
  price FLOAT,
  stock_quantity INT
) USING iceberg;

-- Create Order_Items table if not exists
CREATE TABLE IF NOT EXISTS order_items (
  order_id INT,
  product_id INT,
  quantity INT,
  unit_price FLOAT
) USING iceberg;

-- Insert data into Customers table if it's empty
INSERT INTO customers
SELECT * FROM (
  VALUES 
    (1, 'John Doe', 'john@example.com', '123 Main St'),
    (2, 'Jane Smith', 'jane@example.com', '456 Elm St')
) AS new_customers
WHERE NOT EXISTS (SELECT 1 FROM customers LIMIT 1);

-- Insert data into Products table if it's empty
INSERT INTO products
SELECT * FROM (
  VALUES 
    (1, 'Widget A', 9.99, 100),
    (2, 'Gadget B', 19.99, 50)
) AS new_products
WHERE NOT EXISTS (SELECT 1 FROM products LIMIT 1);

-- Insert data into Orders table if it's empty
INSERT INTO orders
SELECT * FROM (
  VALUES 
    (1, 1, to_date('2023-05-01', 'yyyy-MM-dd'), 'Completed'),
    (2, 2, to_date('2023-05-02', 'yyyy-MM-dd'), 'Processing')
) AS new_orders(order_id, customer_id, order_date, status)
WHERE NOT EXISTS (SELECT 1 FROM orders LIMIT 1);

-- Insert data into Order_Items table if it's empty
INSERT INTO order_items
SELECT * FROM (
  VALUES 
    (1, 1, 2, 9.99),
    (1, 2, 1, 19.99),
    (2, 1, 3, 9.99)
) AS new_order_items
WHERE NOT EXISTS (SELECT 1 FROM order_items LIMIT 1);

-- Add phone_number column to Customers table
ALTER TABLE customers ADD COLUMN phone_number STRING;

-- Rename price column to unit_price in Products table
ALTER TABLE products RENAME COLUMN price TO unit_price;

-- Upsert (merge) example for Products table
MERGE INTO products t
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

-- Insert with a SELECT statement (with condition to avoid duplicates)
INSERT INTO order_items
SELECT o.order_id, p.product_id, 1, p.unit_price
FROM orders o
CROSS JOIN products p
WHERE o.order_id = 2 AND p.product_id = 2
  AND NOT EXISTS (
    SELECT 1 FROM order_items oi
    WHERE oi.order_id = o.order_id AND oi.product_id = p.product_id
  );

-- Optional: Add some SELECT statements to verify the data
SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM orders;
SELECT * FROM order_items;