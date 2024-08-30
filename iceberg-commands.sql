-- Create a new Iceberg table
CREATE TABLE hadoop_catalog.db.sample (
  id INT,
  name STRING,
  value DOUBLE
) USING iceberg;

-- Insert data into the table
INSERT INTO hadoop_catalog.db.sample VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.0), (3, 'Charlie', 15.2);

-- Update data in the table
UPDATE hadoop_catalog.db.sample SET value = value * 2 WHERE id = 2;

-- Delete data from the table
DELETE FROM hadoop_catalog.db.sample WHERE id = 3;

-- Merge data into the table
MERGE INTO hadoop_catalog.db.sample t
USING (SELECT 4 as id, 'David' as name, 30.0 as value) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.value = s.value
WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value);

-- Select data from the table
SELECT * FROM hadoop_catalog.db.sample;

-- Create a partitioned table
CREATE TABLE hadoop_catalog.db.partitioned_sample (
  id INT,
  date DATE,
  name STRING,
  value DOUBLE
) USING iceberg
PARTITIONED BY (date);

-- Insert data into the partitioned table
INSERT INTO hadoop_catalog.db.partitioned_sample 
VALUES (1, '2023-01-01', 'Alice', 10.5),
       (2, '2023-01-02', 'Bob', 20.0),
       (3, '2023-01-01', 'Charlie', 15.2);

-- Perform maintenance tasks
CALL hadoop_catalog.system.rewrite_data_files('db.sample');
CALL hadoop_catalog.system.remove_orphan_files('db.sample');
CALL hadoop_catalog.system.expire_snapshots('db.sample', TIMESTAMP '2023-01-01 00:00:00.000');
