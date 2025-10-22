# Finally, let's augment the claim_policy table with geolocation and materialize it for our dashboard


from pyspark import pipelines as dp
from pyspark.sql.functions import col, pandas_udf
from typing import Iterator
import pandas as pd
import geopy
import random

def geocode(geolocator, address):
    try:
      #Skip the API call for faster demo (remove this line for ream)
      return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
      location = geolocator.geocode(address)
      if location:
          return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
      print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})

@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  #ctx = ssl.create_default_context(cafile=certifi.where())
  #geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
  for address in batch_iter:
    yield address.apply(lambda x: geocode(geolocator, x))


# ==========================================================================
# == MATERIALIZED VIEW: claim_policy_telematics                           ==
# ========================================================================== 
@dp.table(comment="claims with geolocation latitude/longitude")
def claim_policy_telematics():
  t = dp.read("telematics")
  claim = dp.read("claim_policy").where("address is not null")
  return (claim.withColumn("lat_long", get_lat_long(col("address"))).join(t, on="chassis_no"))