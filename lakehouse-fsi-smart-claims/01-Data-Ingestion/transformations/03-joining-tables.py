# Let's now join the tables for a unified view

from pyspark import pipelines as dp

# ==========================================================================
# == STREAMING TABLE: claim_policy                                        ==
# ========================================================================== 
@dp.table(comment = "Curated claim joined with policy records")
def claim_policy():
    # Read the staged policy records
    policy = dp.read("policy")
    # Read the staged claim records
    claim = dp.readStream("claim")
    
    return claim.join(policy, on="policy_no")
