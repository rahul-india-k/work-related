from pyspark.sql import functions as F

# Step 1: Load the data
forecast_volume_df = spark.read.format("jdbc").options(**jdbc_verintwfm_forecast_volume_options).load()
stage_forecast_volume_df = spark.read.format("jdbc").options(**jdbc_verintwfm_stage_forecast_volume_options).load()

# Step 2: DELETE - Filter out records that need to be removed
min_forecast_interval = stage_forecast_volume_df.agg(F.min("ForecastInterval")).collect()[0][0]
max_forecast_interval = stage_forecast_volume_df.agg(F.max("ForecastInterval")).collect()[0][0]

# Identify records for deletion
to_delete_df = forecast_volume_df.join(
    stage_forecast_volume_df,
    (forecast_volume_df["DateTime"] == stage_forecast_volume_df["ForecastInterval"]) &
    (forecast_volume_df["CampaignID"] == stage_forecast_volume_df["CampaignID"]) &
    (forecast_volume_df["QueueID"] == stage_forecast_volume_df["QueueID"]),
    how="left"
).filter(
    (forecast_volume_df["DateTime"] < min_forecast_interval) |
    (forecast_volume_df["DateTime"] > max_forecast_interval) |
    stage_forecast_volume_df["ForecastInterval"].isNull()
)

# Remove these records from the original `forecast_volume_df`
after_delete_df = forecast_volume_df.subtract(to_delete_df)

# Step 3: UPDATE - Update the records based on matching criteria
# Perform the update by joining and replacing certain columns where appropriate
updated_df = after_delete_df.join(
    stage_forecast_volume_df,
    (after_delete_df["DateTime"] == stage_forecast_volume_df["ForecastInterval"]) &
    (after_delete_df["CampaignID"] == stage_forecast_volume_df["CampaignID"]) &
    (after_delete_df["QueueID"] == stage_forecast_volume_df["QueueID"]),
    "left"
).select(
    after_delete_df["DateTime"],
    after_delete_df["CampaignID"],
    after_delete_df["QueueID"],
    F.coalesce(stage_forecast_volume_df["AHT"], after_delete_df["AHT"]).alias("AHT"),
    F.coalesce(stage_forecast_volume_df["CALLVOLUME"], after_delete_df["CALLVOLUME"]).alias("CALLVOLUME"),
    after_delete_df["RecordCreatedDateTime"],
    F.lit(F.current_timestamp()).alias("RecordLastModifiedDateTime")
)

# Step 4: INSERT - Find new records that don’t exist in `after_delete_df` and insert them
# Identify new records in `stage_forecast_volume_df` missing in `after_delete_df`
new_records_df = stage_forecast_volume_df.join(
    after_delete_df,
    (stage_forecast_volume_df["ForecastInterval"] == after_delete_df["DateTime"]) &
    (stage_forecast_volume_df["CampaignID"] == after_delete_df["CampaignID"]) &
    (stage_forecast_volume_df["QueueID"] == after_delete_df["QueueID"]),
    "left_anti"  # Anti-join retrieves records only in `stage_forecast_volume_df` but not in `after_delete_df`
).select(
    stage_forecast_volume_df["ForecastInterval"].alias("DateTime"),
    stage_forecast_volume_df["CampaignID"],
    stage_forecast_volume_df["QueueID"],
    stage_forecast_volume_df["AHT"],
    stage_forecast_volume_df["CALLVOLUME"],
    F.lit(F.current_timestamp()).alias("RecordCreatedDateTime"),
    F.lit(F.current_timestamp()).alias("RecordLastModifiedDateTime")
)

# Step 5: Combine all three: after DELETE, updated records, and new inserts
final_df = updated_df.union(new_records_df)

# Show the final DataFrame
final_df.show()
