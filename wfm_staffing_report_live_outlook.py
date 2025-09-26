from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("StaffingReportConversion").getOrCreate()

# --- Configuration and Constants ---
CURRENT_TIMESTAMP = F.current_timestamp() 
START_DATE = "2025-07-13"
END_DATE = "2025-08-31"

# ASSUMPTION: The SQL's "CROSS APPLY [dbo].[tvf_split_wfm_staffing_map_viewname]" 
# is equivalent to joining with a lookup table. We'll use a placeholder table name.
PARTNER_SITE_LOB_MAP_TABLE = "dbo.partner_site_lob_map"


# --- Helper Function for Time Adjustment (Quarter-Hour Rounding) ---
# SQL: CASE WHEN DATEPART(MINUTE, [DateTime]) IN (15, 45) THEN DATEADD(MINUTE, -15, [DateTime]) ELSE [DateTime] END
def adjust_datetime(col):
    return F.when(
        F.minute(col).isin([15, 45]),
        F.expr(f"date_sub({col}, interval 15 minute)")
    ).otherwise(col)


# ==============================================================================
# 1. BASE DATA LOADING (Initial Staging)
# ==============================================================================

# Read base tables (replace 'dbo.' with your actual catalog/schema prefix)
df_history = spark.read.table("dbo.verintwfm_queue_filters_history")
df_map = spark.read.table(PARTNER_SITE_LOB_MAP_TABLE)
df_fte = spark.read.table("dbo.verintwfm_forecast_fte")
df_queue = spark.read.table("dbo.verintwfm_forecast_queue")
df_volume = spark.read.table("dbo.verintwfm_forecast_volume")
df_staffing_req = spark.read.table("dbo.wfm_staffing_requirement")
df_erlang_req_source = spark.read.table("dbo.erlang_forecast_required")


# ==============================================================================
# 2. TEMPORARY TABLE: #t_queue_filters (Reusable Filter & Map)
# ==============================================================================
# Replicates the SQL join with the CROSS APPLY function.
df_queue_filters = (
    df_history.alias("qf")
    .filter(F.col("FilterName").isin("Staffing Outlook", "EHS Staffing Outlook", "ESS Staffing Outlook"))
    .join(df_map.alias("f"), F.col("qf.ViewName") == F.col("f.ViewName"), "inner")
    .select(
        F.col("qf.ViewName").alias("qf_ViewName"), F.col("qf.QueueID"), 
        F.col("f.Partner"), F.col("f.Site"), F.col("f.LineOfBusiness"), 
        F.col("f.ServiceModel"), F.col("qf.RecordStartDateInclusive"), 
        F.col("qf.RecordEndDateNonInclusive")
    )
).cache()


# ==============================================================================
# 3. TEMPORARY TABLE: #t_forecast_requirement (FTE)
# ==============================================================================
df_forecast_requirement = (
    df_fte.alias("r")
    .join(
        df_queue_filters.alias("m"), 
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
        adjust_datetime(F.col("r.DateTime")).alias("DateTime"), 
        "m.qf_ViewName", "m.Partner", "m.Site", "m.LineOfBusiness", "m.ServiceModel"
    )
    .agg((F.sum("r.FTE") / 2.0).alias("ForecastRequired"))
).cache()


# ==============================================================================
# 4. TEMPORARY TABLE: #t_forecast_queue (AFTE)
# ==============================================================================
df_forecast_queue = (
    df_queue.alias("q")
    .join(
        df_queue_filters.alias("m"), 
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
        adjust_datetime(F.col("q.DateTime")).alias("DateTime"), 
        "m.qf_ViewName", "m.Partner", "m.Site", "m.LineOfBusiness", "m.ServiceModel"
    )
    .agg((F.sum("q.AFTE") / 2.0).alias("ForecastFTE"))
).cache()


# ==============================================================================
# 5. TEMPORARY TABLE: #t_forecast_volume (Volume & AHT)
# ==============================================================================
df_forecast_volume = (
    df_volume.alias("v")
    .join(
        df_queue_filters.alias("m"), 
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
        adjust_datetime(F.col("v.DateTime")).alias("DateTime"), 
        "m.qf_ViewName", "m.Partner", "m.Site", "m.LineOfBusiness", "m.ServiceModel"
    )
    .agg(
        F.sum("v.CALLVOLUME").alias("ForecastVolume"),
        # Corrected AHT Calculation: SUM(Volume * AHT) / NULLIF(SUM(Volume), 0)
        (
            F.sum(F.col("v.CALLVOLUME") * F.col("v.AHT")) 
            / F.nullif(F.sum(F.col("v.CALLVOLUME")), F.lit(0))
        ).cast(DoubleType()).alias("ForecastAHT")
    )
).cache()


# ==============================================================================
# 6. TEMPORARY TABLE: #t_required (Contract Required)
# ==============================================================================
# Replicates the SQL join with the CROSS APPLY function.
df_required = (
    df_staffing_req.alias("r")
    .join(df_map.alias("f"), F.col("r.ViewName") == F.col("f.ViewName"), "inner")
    .filter(
        (F.col("r.DateTime").between(START_DATE, END_DATE))
        & (F.col("r.DateTime") < F.lit("2025-01-12")) # Contradictory date filter from SQL
        & (F.col("r.Required").isNotNull())
        & (F.col("r.Required") > 0)
    )
    .select(
        F.col("r.DateTime"), F.col("r.ViewName"), F.col("f.Partner"), F.col("f.Site"),
        F.col("f.LineOfBusiness"), F.col("f.ServiceModel"),
        F.col("r.Required").alias("ContractRequired")
    )
).cache()


# ==============================================================================
# 7. TEMPORARY TABLE: #t_erlang_required
# ==============================================================================
df_erlang_required = (
    df_erlang_req_source.alias("r")
    .join(
        df_queue_filters.alias("f"), 
        (F.col("r.QueueID") == F.col("f.QueueID")) & 
        (F.col("r.DateTime") >= F.col("f.RecordStartDateInclusive")) & 
        (F.col("r.DateTime") < F.col("f.RecordEndDateNonInclusive")), 
        "inner"
    )
    .filter(F.col("r.DateTime").between(START_DATE, END_DATE))
    .groupBy(
        F.col("r.DateTime"), "f.qf_ViewName", "f.Partner", "f.Site", "f.LineOfBusiness", "f.ServiceModel"
    )
    .agg(F.sum("r.Required").alias("ErlangRequired"))
).cache()


# ==============================================================================
# 8. FINAL JOIN AND SELECT (Combining All Staging Tables)
# ==============================================================================
# The final SQL uses a series of FULL OUTER JOINs.

# List of common keys for joining
join_keys = [
    "DateTime", "ViewName", "Partner", "Site", "LineOfBusiness", "ServiceModel"
]

# Rename columns to a consistent standard for the final join chain
df_fr = df_forecast_requirement.withColumnRenamed("qf_ViewName", "ViewName")
df_fq = df_forecast_queue.withColumnRenamed("qf_ViewName", "ViewName")
df_fv = df_forecast_volume.withColumnRenamed("qf_ViewName", "ViewName")
df_er = df_erlang_required.withColumnRenamed("qf_ViewName", "ViewName")

df_final_report = (
    df_fr
    .join(df_fq, join_keys, "full_outer")
    .join(df_fv, join_keys, "full_outer")
    .join(df_required, join_keys, "full_outer")
    .join(df_er, join_keys, "full_outer")
    .select(
        # The SQL uses ISNULL() on the join keys to resolve the final primary key.
        F.coalesce(F.col("DateTime")).alias("DateTime"),
        F.coalesce(F.col("ViewName")).alias("VerintWfmStaffingMapViewName"),
        F.coalesce(F.col("Partner")).alias("Partner"),
        F.coalesce(F.col("Site")).alias("Site"),
        F.coalesce(F.col("LineOfBusiness")).alias("LineOfBusiness"),
        F.coalesce(F.col("ServiceModel")).alias("ServiceModel"),
        
        # Select and cast the metrics, allowing NULLs to propagate as required.
        F.col("ForecastRequired").cast(DoubleType()),
        F.col("ForecastFTE").cast(DoubleType()),
        F.col("ForecastVolume").cast(DoubleType()),
        F.col("ForecastAHT").cast(DoubleType()),
        F.col("ContractRequired").cast(DoubleType()),
        F.col("ErlangRequired").cast(DoubleType()),
        
        CURRENT_TIMESTAMP.alias("DateCreated")
    )
)


# ==============================================================================
# 9. WRITE FINAL OUTPUT & OPTIMIZE (TRUNCATE + INSERT equivalent)
# ==============================================================================

# SQL TRUNCATE TABLE [dbo].[wfm_staffing_report_live_outlook] is equivalent to 'overwrite' mode
TARGET_TABLE = "dbo.wfm_staffing_report_live_outlook"
df_final_report.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

# SQL CREATE UNIQUE INDEX is replaced by Databricks Z-Ordering optimization
spark.sql(f"""
  OPTIMIZE {TARGET_TABLE}
  ZORDER BY (DateTime, VerintWfmStaffingMapViewName)
""")


# ==============================================================================
# 10. CLEANUP (SQL DROP TABLE equivalent)
# ==============================================================================

# Unpersist cached DataFrames
df_queue_filters.unpersist()
df_forecast_requirement.unpersist()
df_forecast_queue.unpersist()
df_forecast_volume.unpersist()
df_required.unpersist()
df_erlang_required.unpersist()
