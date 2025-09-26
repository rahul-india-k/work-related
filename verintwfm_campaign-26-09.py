from pyspark.sql import functions as F

# Step 1: Load the data
campaign_df = spark.read.format("jdbc").options(**jdbc_verintwfm_campaign_options).load()
stage_campaign_df = spark.read.format("jdbc").options(**jdbc_verintwfm_stage_campaign_options).load()

# Current Timestamp (@now equivalent)
current_timestamp = F.current_timestamp()

# Step 2: Mark Records as Removed (Equivalent to UPDATE WHERE `s.CampaignID IS NULL`)
marked_removed_df = campaign_df.join(
    stage_campaign_df,
    campaign_df["CampaignID"] == stage_campaign_df["CampaignID"],
    how="left"
).withColumn(
    "RecordRemovedDateTime",
    F.when(stage_campaign_df["CampaignID"].isNull(), current_timestamp).otherwise(campaign_df["RecordRemovedDateTime"])
)

# Step 3: Update Existing Records (Check for differences from the staging table)
updated_df = marked_removed_df.join(
    stage_campaign_df,
    marked_removed_df["CampaignID"] == stage_campaign_df["CampaignID"],
    how="inner"
).filter(
    (marked_removed_df["ParentCampaignID"] != stage_campaign_df["ParentCampaignID"]) |
    (marked_removed_df["CampaignName"] != stage_campaign_df["CampaignName"]) |
    (marked_removed_df["CampaignDescription"] != stage_campaign_df["CampaignDescription"]) |
    (marked_removed_df["IsDistributed"] != stage_campaign_df["IsDistributed"]) |
    marked_removed_df["RecordRemovedDateTime"].isNull()
).select(
    marked_removed_df["CampaignID"],
    stage_campaign_df["ParentCampaignID"].alias("ParentCampaignID"),
    stage_campaign_df["CampaignName"].alias("CampaignName"),
    stage_campaign_df["CampaignDescription"].alias("CampaignDescription"),
    stage_campaign_df["IsDistributed"].alias("IsDistributed"),
    marked_removed_df["RecordCreatedDateTime"],
    current_timestamp.alias("RecordLastModifiedDateTime"),
    F.lit(None).alias("RecordRemovedDateTime")
)

# Step 4: Insert New Records (Get records from staging table not in the main table)
new_records_df = stage_campaign_df.join(
    campaign_df,
    stage_campaign_df["CampaignID"] == campaign_df["CampaignID"],
    how="left_anti"  # Left anti join - records only present in staging table
).select(
    stage_campaign_df["CampaignID"],
    stage_campaign_df["ParentCampaignID"],
    stage_campaign_df["CampaignName"],
    stage_campaign_df["CampaignDescription"],
    stage_campaign_df["IsDistributed"],
    current_timestamp.alias("RecordCreatedDateTime"),
    current_timestamp.alias("RecordLastModifiedDateTime"),
    F.lit(None).alias("RecordRemovedDateTime")
)

# Step 5: Combine Updated and Newly Inserted Records with Existing Non-Updated Records
remaining_records_df = marked_removed_df.subtract(updated_df)  # Remaining records not updated

final_df = remaining_records_df.union(updated_df).union(new_records_df)

# Show the final DataFrame
final_df.show()

# Save the combined DataFrame back to the database
final_df.write.format("jdbc").options(**jdbc_verintwfm_campaign_options).mode("overwrite").save()
