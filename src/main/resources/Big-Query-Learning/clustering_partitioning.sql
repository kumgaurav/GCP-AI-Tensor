SELECT 
  dataset_id,
  table_id,
  -- Convert bytes to GB.
  ROUND(size_bytes/pow(10,9),2) as size_gb,
  -- Convert UNIX EPOCH to a timestamp.
  TIMESTAMP_MILLIS(creation_time) AS creation_time,
  TIMESTAMP_MILLIS(last_modified_time) as last_modified_time,
  row_count,
  CASE 
    WHEN type = 1 THEN 'table'
    WHEN type = 2 THEN 'view'
  ELSE NULL
  END AS type
FROM
  `dw-workshop.tpcds_2t_baseline.__TABLES__`
ORDER BY size_gb DESC

===============================================

SELECT * FROM 
 `dw-workshop.tpcds_2t_baseline.INFORMATION_SCHEMA.COLUMNS`
 
==================================================

Are any of the columns of data in this baseline dataset partitioned or clustered?

SELECT * FROM 
 `dw-workshop.tpcds_2t_baseline.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL
  
=====================================================

How many columns of data does each table have (sorted by most to least?)
Which table has the most columns of data?

SELECT 
  COUNT(column_name) AS column_count, 
  table_name 
FROM 
 `dw-workshop.tpcds_2t_baseline.INFORMATION_SCHEMA.COLUMNS`
GROUP BY table_name
ORDER BY column_count DESC, table_name

=======================================================

