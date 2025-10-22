# Let's start out by ingesting our raw files from our UC volume

from pyspark import pipelines as dp
from utilities.utils import *


# ==========================================================================
# == Incrementally load RAW CLAIMS from JSON                              ==
# ==========================================================================
@dp.table(comment="The raw claims data loaded from json files.")
def raw_claim():
  return (
    spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .load(f"/Volumes/{catalog}/{db}/{volume_name}/Claims")
  )

# ==========================================================================
# == Incrementally load RAW POLICIES from CSV                               ==
# ==========================================================================
@dp.table(comment="Policy data loaded from csv files.")
def raw_policy():
    return (
      spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaHints", "ZIPCODE int")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"/Volumes/{catalog}/{db}/{volume_name}/Policies")
    )

# ==========================================================================
# == Incrementally load RAW TELEMATICS from parquet                       ==
# ==========================================================================
@dp.table(comment="Load Telematics (IoT) streaming data")
def raw_telematics():
  return (
    spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "parquet")
          .load(f"/Volumes/{catalog}/{db}/{volume_name}/Telematics")
  )
