# Let's then clean up our raw tables into new streaming tables

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, col
from utilities import utils

# ==========================================================================
# == STREAMING TABLE: telematics                                          ==
# ==========================================================================
@dp.table(comment="Average telematics")
def telematics():
  return (dp.read('raw_telematics').groupBy("chassis_no").agg(
                F.avg("speed").alias("telematics_speed"),
                F.avg("latitude").alias("telematics_latitude"),
                F.avg("longitude").alias("telematics_longitude")))

# ==========================================================================
# == STREAMING TABLE: policy.                                             ==
# ==========================================================================          
@dp.table
@dp.expect_all({"valid_policy_number": "policy_no IS NOT NULL"})
def policy():
    # Read the staged policy records into memory
    return (dp.readStream("raw_policy")
                .withColumn("premium", F.abs(col("premium")))
                # Reformat the incident date values
                .withColumn("pol_eff_date", F.to_date(col("pol_eff_date"), "dd-MM-yyyy"))
                .withColumn("pol_expiry_date", F.to_date(col("pol_expiry_date"), "dd-MM-yyyy"))
                .withColumn("pol_issue_date", F.to_date(col("pol_issue_date"), "dd-MM-yyyy"))
                .withColumn("address", concat(col("BOROUGH"), lit(", "), col("ZIP_CODE").cast("string")))
                .drop('_rescued_data'))

# ==========================================================================
# == STREAMING TABLE: claim.                                              ==
# ========================================================================== 
@dp.table
@dp.expect_all({"valid_claim_number": "claim_no IS NOT NULL"})
def claim():
    # Read the staged claim records into memory
    claim = dp.readStream("raw_claim")
    claim = utils.flatten_struct(claim)  
    
    # Update the format of all date/time features
    return (claim.withColumn("claim_date", F.to_date(F.col("claim_date")))
                 .withColumn("incident_date", F.to_date(F.col("incident_date"), "yyyy-MM-dd"))
                 .withColumn("driver_license_issue_date", F.to_date(F.col("driver_license_issue_date"), "dd-MM-yyyy"))
                 .drop('_rescued_data'))