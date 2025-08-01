# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

fire_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(fire_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####fix column names with space

# COMMAND ----------

renamed_fire_df = fire_df \
    .withColumnRenamed("Call Number", "call_number") \
    .withColumnRenamed("Unit ID", "unit_id") \
    .withColumnRenamed("Incident Number", "incident_number") \
    .withColumnRenamed("Call Type", "call_type") \
    .withColumnRenamed("Call Date", "call_date") \
    .withColumnRenamed("Watch Date", "watch_date") \
    .withColumnRenamed("Call Final Disposition", "call_final_disposition") \
    .withColumnRenamed("Available DtTm", "available_dt_tm") \
    .withColumnRenamed("Address", "address") \
    .withColumnRenamed("City", "city") \
    .withColumnRenamed("Zipcode of Incident", "zipcode_of_incident") \
    .withColumnRenamed("Battalion", "battalion") \
    .withColumnRenamed("Station Area", "station_area") \
    .withColumnRenamed("Box", "box") \
    .withColumnRenamed("OrigPriority", "orig_priority") \
    .withColumnRenamed("Priority", "priority") \
    .withColumnRenamed("Final Priority", "final_priority") \
    .withColumnRenamed("ALS Unit", "als_unit") \
    .withColumnRenamed("Call Type Group", "call_type_group") \
    .withColumnRenamed("NumAlarms", "num_alarms") \
    .withColumnRenamed("UnitType", "unit_type") \
    .withColumnRenamed("Unit sequence in call dispatch", "unit_sequence_in_call_dispatch") \
    .withColumnRenamed("Fire Prevention District", "fire_prevention_district") \
    .withColumnRenamed("Supervisor District", "supervisor_district") \
    .withColumnRenamed("Neighborhood", "neighborhood") \
    .withColumnRenamed("Location", "location") \
    .withColumnRenamed("RowID", "row_id") \
    .withColumnRenamed("Delay", "delay")
display(renamed_fire_df)

# COMMAND ----------

renamed_fire_df.printSchema()

# COMMAND ----------

fire_new_df = renamed_fire_df \
    .withColumn("call_date", to_date("call_date", "MM/dd/yyyy")) \
    .withColumn("watch_date", to_date("watch_date", "MM/dd/yyyy")) \
    .withColumn("available_dt_tm", to_timestamp("available_dt_tm", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("delay", round("delay", 2))
     

# COMMAND ----------

display(fire_new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Approach:
# MAGIC convert the dataframe to temporary view and run sql on the view

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. How many distinct types of calls were made to the Fire Department?
# MAGIC select count(distinct CallType) as distinct_call_type_count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null

# COMMAND ----------

fire_new_df.createOrReplaceTempView("fire_service_calls_view")
q1_sql_df = spark.sql("""
        select count(distinct CallType) as distinct_call_type_count
        from fire_service_calls_view
        where CallType is not null
        """)
display(q1_sql_df)
     

# COMMAND ----------

q1_df = fire_new_df.where("CallType is not null") \
            .select("CallType") \
            .distinct()
print(q1_df.count())

# COMMAND ----------

q2_new_df = fire_new_df.where("calltype is not null") \
            .select(expr("calltype as distinct_call_type")) \
            .distinct()
q2_new_df.show()

# COMMAND ----------

display(q2_new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Find out all response for delayed times greater than 5 mins?
# MAGIC select CallNumber, Delay
# MAGIC from fire_service_calls_tbl
# MAGIC where Delay > 5

# COMMAND ----------

q3_df = fire_new_df.where("delay > 5") \
            .select("call_number", "delay")
q3_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Q4. What were the most common call types?
# MAGIC select CallType, count(*) as count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null
# MAGIC group by CallType
# MAGIC order by count desc

# COMMAND ----------

q4_df = fire_new_df.select("calltype") \
            .where("calltype is not null") \
            .groupBy("calltype") \
            .count() \
            .orderBy(desc("count")) \
            .show()


# COMMAND ----------

