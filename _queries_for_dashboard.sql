-- Databricks notebook source
-- DBTITLE 1,DLT-retail-data-quality-per-table
select 'passed_records' as type,
       sum(passed_records) as value, 
       sum(failed_records)/sum(output_records)*100 as failure_rate,
       dataset 
   from hive_metastore.dbdemos.dlt_expectations  group by dataset
union
select 'failed_records' as type,
       sum(failed_records) as value, 
       sum(failed_records)/sum(output_records)*100 as failure_rate,
       dataset 
   from hive_metastore.dbdemos.dlt_expectations  group by dataset
union
select 'dropped_records' as type,
       sum(dropped_records) as value, 
       sum(failed_records)/sum(output_records)*100 as failure_rate,
       dataset 
   from hive_metastore.dbdemos.dlt_expectations  group by dataset


-- COMMAND ----------

-- DBTITLE 1,DLT-retail-data-quality-stats
select 
    date(timestamp) day, sum(failed_records)/sum(output_records)*100 failure_rate, sum(output_records) output_records from hive_metastore.dbdemos.dlt_expectations 
group by day order by day
