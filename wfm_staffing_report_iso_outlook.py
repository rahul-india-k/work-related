from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Define the common date range filter
START_DATE = "2025-07-13"
END_DATE = "2025-08-31"


# ==============================================================================
# 1. TEMPORARY TABLE: [#t_queue_filters] (Base for Joins)
# ==============================================================================
# The SQL requires two steps: read the history table AND read/join the view name split logic.
# ASSUMPTION: The 'tvf_split_wfm_staffing_map_viewname' logic is replicated by joining 
# to a lookup table (partner_site_lob_map) based on ViewName.

# This table represents the result of the SQL logic on verintwfm_queue_filters_history
queue_filters_df = spark.read.table("dbo.verintwfm_queue_filters_history")
partner_site_lob_map_df = spark.read.table("dbo.partner_site_lob_map") # ASSUMED table for CROSS APPLY logic

queue_filters_df = (
    queue_filters_df
    .filter(F.col("FilterName").isin("Staffing Outlook", "EHS Staffing Outlook", "ESS Staffing Outlook"))
    .join(partner_site_lob_map_df, "ViewName", "inner") # Replicates the CROSS APPLY
    .select(
        F.col("ViewName"), F.col("QueueID"), F.col("Partner"), F.col("Site"), 
        F.col("LineOfBusiness"), F.col("ServiceModel"),
        F.col("RecordStartDateInclusive"), F.col("RecordEndDateNonInclusive")
    )
    .withColumnRenamed("ViewName", "qf_ViewName")
)
# Cache the filter table as it is reused in every stage
queue_filters_df.cache() 


# ==============================================================================
# 2. TEMPORARY TABLE: [#t_forecast_requirement]
# ==============================================================================
forecast_fte_source_df = spark.read.table("dbo.verintwfm_forecast_fte")

# Time adjustment logic (rounding down to nearest 30-min block)
time_adjustment_col = F.when(
        F.minute("DateTime").isin([15, 45]),
        F.expr("date_add(DateTime, interval -15 minute)")
    ).otherwise(F.col("DateTime")).alias("AdjustedDateTime")

forecast_requirement_df = (
    forecast_fte_source_df.alias("r")
    .join(
        queue_filters_df.alias("m"), 
        (F.col("r.QueueID") == F.col("m.QueueID")) & 
        (F.col("r.DateTime") >= F.col("m.RecordStartDateInclusive")) & 
        (F.col("r.DateTime") < F.col("m.RecordEndDateNonInclusive")), 
        "inner"
    )
    .filter(
        (F.col("r.DateTime").between(START_DATE, END_DATE))
        & (F.col("r.FTE").isNotNull()) 
        & (F.col("r.FTE") > 0)
    )
    .groupBy(
        time_adjustment_col, "m.qf_ViewName", "m.Partner", "m.Site", "m.LineOfBusiness", "m.ServiceModel"
    )
    .agg((F.sum("r.FTE") / 2.0).alias("ForecastRequired")) # Division by 2.0 applied here, as per SQL
)
forecast_requirement_df.cache()


# ==============================================================================
# 3. TEMPORARY TABLE: [#t_forecast_queue]
# ==============================================================================
# NOTE: This uses a different source table than [#t_forecast_requirement]
forecast_queue_source_df = spark.read.table("dbo.verintwfm_forecast_queue")

forecast_queue_df = (
    forecast_queue_source_df.alias("q")
    .join(
        queue_filters_df.alias("m"), 
        (F.col("q.QueueID") == F.col("m.QueueID")) & 
        (F.col("q.DateTime") >= F.col("m.RecordStartDateInclusive")) & 
        (F.col("q.DateTime") < F.col("m.RecordEndDateNonInclusive")), 
        "inner"
    )
    .filter(
        (F.col("q.DateTime").between(START_DATE, END_DATE))
        & (F.col("q.AFTE").isNotNull()) 
        & (F.col("q.AFTE") > 0)
    )
    .groupBy(
        time_adjustment_col, "m.qf_ViewName", "m.Partner", "m.Site", "m.LineOfBusiness", "m.ServiceModel"
    )
    .agg((F.sum("q.AFTE") / 2.0).alias("ForecastFTE"))
)
forecast_queue_df.cache()


# ==============================================================================
# 4. TEMPORARY TABLE: [#t_forecast_volume]
# ==============================================================================
forecast_volume_source_df = spark.read.table("dbo.verintwfm_forecast_volume")

forecast_volume_df = (
    forecast_volume_source_df.alias("v")
    .join(
        queue_filters_df.alias("m"), 
        (F.col("v.QueueID") == F.col("m.QueueID")) & 
        (F.col("v.DateTime") >= F.col("m.RecordStartDateInclusive")) & 
        (F.col("v.DateTime") < F.col("m.RecordEndDateNonInclusive")), 
        "inner"
    )
    .filter(
        (F.col("v.DateTime").between(START_DATE, END_DATE))
        & (F.col("v.CALLVOLUME").isNotNull()) 
        & (F.col("v.CALLVOLUME") > 0)
    )
    .groupBy(
        time_adjustment_col, "m.qf_ViewName", "m.Partner", "m.Site", "m.LineOfBusiness", "m.ServiceModel"
    )
    .agg(
        F.sum("v.CALLVOLUME").alias("ForecastVolume"),
        # Corrected AHT Calculation: SUM(Volume * AHT) / SUM(Volume)
        (F.sum(F.col("v.CALLVOLUME") * F.col("v.AHT")) / F.nullif(F.sum(F.col("v.CALLVOLUME")), F.lit(0))).alias("ForecastAHT")
    )
)
forecast_volume_df.cache()


# ==============================================================================
# 5. TEMPORARY TABLE: [#t_required] (Contract Required)
# ==============================================================================
required_source_df = spark.read.table("dbo.wfm_staffing_requirement")

required_df = (
    required_source_df.alias("r")
    .join(partner_site_lob_map_df.alias("f"), "ViewName", "inner") # Replicates the CROSS APPLY logic from SQL
    .filter(
        (F.col("r.DateTime").between(START_DATE, END_DATE))
        & (F.col("r.DateTime") < F.lit("2025-01-12")) # NOTE: This contradictory filter is retained for fidelity
        & (F.col("r.Required").isNotNull())
        & (F.col("r.Required") > 0)
    )
    .select(
        F.col("r.DateTime"), F.col("r.ViewName"), F.col("f.Partner"), F.col("f.Site"),
        F.col("f.LineOfBusiness"), F.col("f.ServiceModel"),
        F.col("r.Required").alias("ContractRequired")
    )
)
required_df.cache()


# ==============================================================================
# 6. TEMPORARY TABLE: [#t_erlang_required]
# ==============================================================================
erlang_required_source_df = spark.read.table("dbo.erlang_forecast_required")

erlang_required_df = (
    erlang_required_source_df.alias("r")
    .join(
        queue_filters_df.alias("f"), 
        (F.col("r.QueueID") == F.col("f.QueueID")) & 
        (F.col("r.DateTime") >= F.col("f.RecordStartDateInclusive")) & 
        (F.col("r.DateTime") < F.col("f.RecordEndDateNonInclusive")), 
        "inner"
    )
    .filter(F.col("r.DateTime").between(START_DATE, END_DATE))
    .groupBy(
        "r.DateTime", "f.qf_ViewName", "f.Partner", "f.Site", "f.LineOfBusiness", "f.ServiceModel"
    )
    .agg(F.sum("r.Required").alias("ErlangRequired"))
    .withColumnRenamed("qf_ViewName", "ViewName") # Rename for final join
)
erlang_required_df.cache()


# ==============================================================================
# 7. FINAL TABLE CREATION: [dbo].[wfm_staffing_report_live_outlook]
# ==============================================================================
# Define the common keys for the full outer joins
join_keys = [
    "DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"
]

# The final query is a chain of FULL OUTER JOINs
final_report_df = (
    forecast_requirement_df.withColumnRenamed("AdjustedDateTime", "DateTime") # Aligning key names
    .withColumnRenamed("qf_ViewName", "ViewName")
    
    .join(forecast_queue_df.withColumnRenamed("AdjustedDateTime", "DateTime").withColumnRenamed("qf_ViewName", "ViewName"), join_keys, "full_outer")
    .join(forecast_volume_df.withColumnRenamed("AdjustedDateTime", "DateTime").withColumnRenamed("qf_ViewName", "ViewName"), join_keys, "full_outer")
    .join(required_df, join_keys, "full_outer")
    .join(erlang_required_df, join_keys, "full_outer")

    .select(
        F.coalesce(F.col("DateTime_fr"), F.col("DateTime_fq"), F.col("DateTime_fv"), F.col("DateTime_r"), F.col("DateTime")).alias("DateTime"), # Coalesce the primary key from all sources
        F.coalesce(F.col("ViewName_fr"), F.col("ViewName_fq"), F.col("ViewName_fv"), F.col("ViewName_r"), F.col("ViewName")).alias("VerintWfmStaffingMapViewName"),
        F.coalesce(F.col("Partner_fr"), F.col("Partner_fq"), F.col("Partner_fv"), F.col("Partner_r"), F.col("Partner")).alias("Partner"),
        F.coalesce(F.col("Site_fr"), F.col("Site_fq"), F.col("Site_fv"), F.col("Site_r"), F.col("Site")).alias("Site"),
        F.coalesce(F.col("LineOfBusiness_fr"), F.col("LineOfBusiness_fq"), F.col("LineOfBusiness_fv"), F.col("LineOfBusiness_r"), F.col("LineOfBusiness")).alias("LineOfBusiness"),
        F.coalesce(F.col("ServiceModel_fr"), F.col("ServiceModel_fq"), F.col("ServiceModel_fv"), F.col("ServiceModel_r"), F.col("ServiceModel")).alias("ServiceModel"),
        
        # Select the actual metric columns
        F.col("ForecastRequired").cast(DoubleType()),
        F.col("ForecastFTE").cast(DoubleType()),
        F.col("ForecastVolume").cast(DoubleType()),
        F.col("ForecastAHT").cast(DoubleType()),
        F.col("ContractRequired").cast(DoubleType()),
        F.col("ErlangRequired").cast(DoubleType()),
        F.current_timestamp().alias("DateCreated")
    )
)


# ==============================================================================
# 8. WRITE FINAL OUTPUT & OPTIMIZE (SQL equivalent of TRUNCATE + INSERT)
# ==============================================================================

# Write to Delta Lake (recommended Databricks output)
# Since the SQL does a TRUNCATE + INSERT, 'overwrite' mode is the equivalent.
final_report_df.write.format("delta").mode("overwrite").saveAsTable("dbo.wfm_staffing_report_live_outlook")

# Apply Z-Ordering (Optimization to replace SQL Indexes)
# Index your table on the key columns used for grouping and joining.
spark.sql("""
  OPTIMIZE dbo.wfm_staffing_report_live_outlook
  ZORDER BY (DateTime, VerintWfmStaffingMapViewName)
""")

# ==============================================================================
# 9. CLEANUP (SQL equivalent of DROP TABLE)
# ==============================================================================
# Cleanup is done by unpersisting cached DataFrames.
# (Note: Temporary Views would be automatically dropped on session end)
queue_filters_df.unpersist()
forecast_requirement_df.unpersist()
forecast_queue_df.unpersist()
forecast_volume_df.unpersist()
required_df.unpersist()
erlang_required_df.unpersist()



============================================================================================================================================================
============================================================================================================================================================
============================================================================================================================================================
this is given by chatgpt - 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("SQL_to_PySpark").getOrCreate()

# Step 1: Variable Declaration and Current Timestamp
current_timestamp = F.current_timestamp()  # equivalent to @now = GETDATE()

# Step 2: Temporary Table 1 - @t_queue_filters
# Emulate @t_queue_filters being populated
queue_filters_df = spark.read.format("jdbc").options(**jdbc_queue_filters_options).load()
queue_filters_df = queue_filters_df.filter(
    queue_filters_df["FilterName"].isin("Staffing Outlook", "ESS Staffing Outlook")
)
queue_filters_df = queue_filters_df.select(
    F.col("ViewName"),
    F.col("QueueID"),
    F.col("Partner"),
    F.col("Site"),
    F.col("LineOfBusiness"),
    F.col("ServiceModel"),
    F.col("RecordStartDateInclusive"),
    F.col("RecordEndDateNonInclusive"),
)
queue_filters_df.cache()  # Cache since this is reused multiple times

# Step 3: Temporary Table 2 - @t_forecast_requirement
forecast_fte_df = spark.read.format("jdbc").options(**jdbc_forecast_fte_options).load()
forecast_requirement_df = (
    forecast_fte_df.join(
        queue_filters_df, ["QueueID"], "inner"
    )
    .filter(
        (F.col("RecordStartDateInclusive") <= F.col("DateTime"))
        & (F.col("RecordEndDateNonInclusive") > F.col("DateTime"))
        & (F.col("FTE").isNotNull())
        & (F.col("FTE") > 0)
    )
    .groupBy(
        "DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"
    )
    .agg(F.sum("FTE").alias("ForecastRequirement"))
)
forecast_requirement_df.cache()

# Step 4: Temporary Table 3 - @t_forecast_queue
forecast_queue_df = forecast_requirement_df.withColumn(
    "AdjustedDatetime",
    F.when(
        F.minute("DateTime").isin([15, 45]),
        F.expr("date_add(DateTime, interval -15 minute)"),
    ).otherwise(F.col("DateTime")),
)
forecast_queue_df = (
    forecast_queue_df.groupBy(
        "AdjustedDatetime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"
    )
    .agg((F.sum("ForecastRequirement") / 2.0).alias("ForecastFTE"))
)
forecast_queue_df.cache()

# Step 5: Temporary Table 4 - @t_forecast_volume
forecast_volume_df = (
    forecast_queue_df.join(queue_filters_df, ["ViewName", "LineOfBusiness"], "inner")
    .filter(F.col("DateTime").between("2025-07-13", "2025-08-31"))
    .groupBy(
        "DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"
    )
    .agg(
        F.sum(F.col("CALLVOLUME")).alias("ForecastVolume"),
        (F.sum("CALLVOLUME") / F.sum("AHT")).alias("ForecastAHT"),
    )
)
forecast_volume_df.cache()

# Step 6: Temporary Table 5 - @t_required
required_df = (
    queue_filters_df.join(forecast_volume_df, ["QueueID"], "inner")
    .filter((F.col("Required") > 0) & (F.col("Required").isNotNull()))
    .groupBy(
        "DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"
    )
    .agg(F.sum("Required").alias("ContractualRequired"))
)
required_df.cache()

# Step 7: Temporary Table 6 - @t_erlang_required
erlang_required_df = (
    forecast_fte_df.join(queue_filters_df, ["QueueID"], "inner")
    .filter(
        (F.col("DateTime").between("2025-07-13", "2025-08-31"))
        & (F.col("RequiredFTE").isNotNull())
    )
    .groupBy(
        "DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"
    )
    .agg(F.sum("RequiredFTE").alias("ErlangRequired"))
)
erlang_required_df.cache()

# Step 8: Final Table Creation - wfm_staffing_report_iso_outlook
final_report_df = (
    forecast_queue_df.join(
        forecast_volume_df,
        ["DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"],
        "full_outer",
    )
    .join(
        required_df,
        ["DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"],
        "full_outer",
    )
    .join(
        erlang_required_df,
        ["DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"],
        "full_outer",
    )
    .select(
        F.col("DateTime"),
        F.col("ViewName"),
        F.col("Partner"),
        F.col("Site"),
        F.col("LineOfBusiness"),
        F.col("ServiceModel"),
        F.coalesce(F.col("ForecastFTE"), F.lit(0)).alias("ForecastFTE"),
        F.coalesce(F.col("ForecastRequirement"), F.lit(0)).alias("ForecastRequirement"),
        F.coalesce(F.col("ForecastVolume"), F.lit(0)).alias("ForecastVolume"),
        F.coalesce(F.col("ForecastAHT"), F.lit(0)).alias("ForecastAHT"),
        F.coalesce(F.col("ContractualRequired"), F.lit(0)).alias("ContractualRequired"),
        F.coalesce(F.col("ErlangRequired"), F.lit(0)).alias("ErlangRequired"),
        current_timestamp.alias("RecordCreatedDateTime"),
    )
)

# Step 9: Write Final Output
final_report_df.write.format("jdbc").options(**jdbc_output_options).mode("overwrite").save()

# Step 10: Cleanup - Unpersist Temporary DataFrames
queue_filters_df.unpersist()
forecast_requirement_df.unpersist()
forecast_queue_df.unpersist()
forecast_volume_df.unpersist()
required_df.unpersist()
erlang_required_df.unpersist()

