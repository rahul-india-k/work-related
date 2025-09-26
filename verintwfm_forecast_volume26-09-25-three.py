from pyspark.sql import functions as F
from pyspark.sql import Window

# Step 1: Load the DataFrames
forecast_volume_df = spark.read.format("jdbc").options(**jdbc_verintwfm_forecast_volume_options).load()
stage_forecast_volume_df = spark.read.format("jdbc").options(**jdbc_verintwfm_stage_forecast_volume_options).load()

# Current Timestamp (@now equivalent)
current_timestamp = F.current_timestamp()

# Step 2: DELETE - Remove records that no longer exist
min_forecast_interval = stage_forecast_volume_df.agg(F.min("ForecastInterval")).collect()[0][0]
max_forecast_interval = stage_forecast_volume_df.agg(F.max("ForecastInterval")).collect()[0][0]

# Identify records for deletion by performing a LEFT OUTER JOIN and filtering
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

# Remove records marked for deletion
after_delete_df = forecast_volume_df.subtract(to_delete_df)

# Step 3: UPDATE - Update records where matching criteria exist
updated_df = after_delete_df.join(
    stage_forecast_volume_df,
    (after_delete_df["DateTime"] == stage_forecast_volume_df["ForecastInterval"]) &
    (after_delete_df["CampaignID"] == stage_forecast_volume_df["CampaignID"]) &
    (after_delete_df["QueueID"] == stage_forecast_volume_df["QueueID"]),
    "left"
).filter(
    (F.col("after_delete_df.AHT").isNotNull() != F.col("stage_forecast_volume.AHT").isNotNull()) |
    (F.col("after_delete_df.CALLVOLUME").isNotNull() != F.col("stage_forecast_volume.CALLVOLUME").isNotNull())
).select(
    after_delete_df["DateTime"],
    after_delete_df["CampaignID"],
    after_delete_df["QueueID"],
    F.coalesce(stage_forecast_volume_df["AHT"], after_delete_df["AHT"]).alias("AHT"),
    F.coalesce(stage_forecast_volume_df["CALLVOLUME"], after_delete_df["CALLVOLUME"]).alias("CALLVOLUME"),
    after_delete_df["RecordCreatedDateTime"],
    current_timestamp.alias("RecordLastModifiedDateTime")  # RecordLastModifiedDateTime set to current timestamp
)

# Step 4: INSERT - Add records from `stage_forecast_volume_df` that do not exist in `forecast_volume_df`
new_records_df = stage_forecast_volume_df.join(
    forecast_volume_df,
    (stage_forecast_volume_df["ForecastInterval"] == forecast_volume_df["DateTime"]) &
    (stage_forecast_volume_df["CampaignID"] == forecast_volume_df["CampaignID"]) &
    (stage_forecast_volume_df["QueueID"] == forecast_volume_df["QueueID"]),
    "left_anti"  # Retrieves records in `stage_forecast_volume_df` but not in `forecast_volume_df`
).select(
    stage_forecast_volume_df["ForecastInterval"].alias("DateTime"),
    stage_forecast_volume_df["CampaignID"],
    stage_forecast_volume_df["QueueID"],
    stage_forecast_volume_df["AHT"],
    stage_forecast_volume_df["CALLVOLUME"],
    current_timestamp.alias("RecordCreatedDateTime"),
    current_timestamp.alias("RecordLastModifiedDateTime"),
)

# Combine DELETE (remaining records), UPDATE (updated records), and INSERT (new records)
final_df = after_delete_df.union(updated_df).union(new_records_df)

# Show the final DataFrame
final_df.show()

# To save the final DataFrame back to the database or data store
final_df.write.format("jdbc").options(**jdbc_verintwfm_forecast_volume_options).mode("overwrite").save()
