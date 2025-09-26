from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read data into DataFrames
campaign_df = spark.read.format("jdbc").options(**jdbc_campaign_options).load()
sp_df = spark.read.format("jdbc").options(**jdbc_sp_options).load()
forecast_instance_df = spark.read.format("jdbc").options(**jdbc_forecast_instance_options).load()
forecast_series_df = spark.read.format("jdbc").options(**jdbc_forecast_series_options).load()
queue_df = spark.read.format("jdbc").options(**jdbc_queue_options).load()

# Filter forecast_series based on DATE constraints (equivalent to WHERE clause)
forecast_series_df = forecast_series_df.filter(
    (F.col("DATETIME") >= F.lit("2023-03-08")) & (F.col("DATETIME") <= F.lit("2023-04-07"))
)

# Join the DataFrames step by step (equivalent to INNER JOINs in SQL)
joined_df = (
    campaign_df.alias("c")
    .join(sp_df.alias("sp"), F.col("c.ID") == F.col("sp.CAMPAIGNID"), "inner")
    .join(forecast_instance_df.alias("fi"), F.col("sp.ID") == F.col("fi.SPID"), "inner")
    .filter(F.col("fi.ISBASE") == 1)
    .join(forecast_series_df.alias("fts"), F.col("fi.ID") == F.col("fts.FORECASTINSTANCEID"), "inner")
    .join(queue_df.alias("q"), F.col("fts.SQUEUEID") == F.col("q.ID"), "inner")
)

# Select and format necessary fields
selected_df = joined_df.select(
    F.col("c.ID").alias("CampaignID"),
    F.col("c.NAME").alias("CampaignName"),
    F.col("q.ID").alias("QueueID"),
    F.col("q.NAME").alias("QueueName"),
    F.col("fts.DATETIME").alias("BaseDateTime"),
    *[F.col(f"fts.AHT{i}").alias(f"AHT{i}") for i in range(1, 97)],
    *[F.col(f"fts.CALLVOLUME{i}").alias(f"CallVolume{i}") for i in range(1, 97)],
)

# Unpivot AHT columns (equivalent to UNPIVOT in SQL)
aht_cols = [f"AHT{i}" for i in range(1, 97)]
callvolume_cols = [f"CallVolume{i}" for i in range(1, 97)]

aht_unpivoted_df = (
    selected_df.select(
        F.col("CampaignID"),
        F.col("CampaignName"),
        F.col("QueueID"),
        F.col("QueueName"),
        F.col("BaseDateTime"),
        F.explode(F.array(*[F.struct(F.lit(c).alias("HandleTimes"), F.col(c)) for c in aht_cols])).alias("AHT_Unpivoted")
    )
    .select(
        "CampaignID",
        "CampaignName",
        "QueueID",
        "QueueName",
        "BaseDateTime",
        F.col("AHT_Unpivoted.HandleTimes").alias("AHT"),
        F.col("AHT_Unpivoted.CallVolume")
    )
)

callvolume_unpivoted_df = (
    selected_df.select(
        F.col("CampaignID"),
        F.col("CampaignName"),
        F.col("QueueID"),
        F.col("QueueName"),
        F.col("BaseDateTime"),
        F.explode(F.array(*[F.struct(F.lit(c).alias("Volumes"), F.col(c)) for c in callvolume_cols])).alias("CallVolume_Unpivoted")
    )
    .select(
        "CampaignID",
        "CampaignName",
        "QueueID",
        "QueueName",
        "BaseDateTime",
        F.col("CallVolume_Unpivoted.Volumes").alias("CallVolume"),
        F.col("CallVolume_Unpivoted.Value")
    )
)

# Combine the AHT and CallVolume unpivoted data with conditions
combined_df = aht_unpivoted_df.join(
    callvolume_unpivoted_df,
    (F.col("BaseDateTime") == F.col("BaseDateTime")) & (F.col("AHT") == F.col("Volumes")),
    "inner"
)

# Further filter the data based on the final WHERE conditions
final_df = combined_df.filter(
    (F.col("CallVolume") >= 0)
    & (~(
        (F.col("BaseDateTime").between("2022-11-06 02:00:00", "2022-11-06 02:45:00"))
        | (F.col("BaseDateTime").between("2023-03-13 04:00:00", "2023-03-13 04:45:00"))
    ))
)

# Show the result
final_df.show()
