# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring the raw data
# MAGIC
# MAGIC For this demo, we've generated some data and stored it in a Unity Catalog Volume. Volumes can store any type of file and can either be managed by Unity Catalog or connected to cloud storage. Lakeflow Declaritive Pipelines can automatically pick up new files and incrementally process data in a volume making your pipelines fast and efficient.
# MAGIC
# MAGIC Let's start by taking a look at the contents of the `raw_data` volume.
# MAGIC
# MAGIC **Note: this notebook is a simple Exploration Notebook, it's not part of our final Pipeline!**
# MAGIC
# MAGIC Having a notebook on the side to test SQL queries interactively can be very handy to accelerate exploration and build your pipelines faster!

# COMMAND ----------

import os
import sys

raw_data_volume = "/Volumes/alex_feng/smart_claims/volume_claims/"

# Print out a list of directories in our raw_data volume and a few files from those directories
for table in os.listdir(raw_data_volume):
  print(table + "/")
  for file in os.listdir(raw_data_volume + table)[:3]:
    print("  " + file)
  print("  ...")


# COMMAND ----------

# MAGIC %md
# MAGIC It looks like we've got a few directories here with `csv` and `parquet` files in them. Let's start by taking a look at the maintenance logs files using the SQL `read_files` function.
# MAGIC
# MAGIC `read_files` supports several different file formats including `csv` and `partquet`. Take a look at the [Databricks documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) to see the available formats and options.
# MAGIC
# MAGIC Additionally, using the `STREAM` keyword `read_files` can be used in streaming tables to ingest files into Delta Lake. `read_files` leverages Auto Loader when used in a streaming table query.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files("/Volumes/alex_feng/smart_claims/volume_claims/Policies/*.csv", format => "csv") limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files("/Volumes/alex_feng/smart_claims/volume_claims/Telematics/*.parquet", format => "parquet") limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files("/Volumes/alex_feng/smart_claims/volume_claims/Claims/*.json", format => "json") limit 10

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: Building our Declarative Pipeline
# MAGIC We now have a good idea of our raw data and the queries we'll have to do!
# MAGIC
# MAGIC It's time to start building our pipeline!
# MAGIC
# MAGIC Open the [00-pipeline-tutorial notebook]($../transformations/00-pipeline-tutorial).
