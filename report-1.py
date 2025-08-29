today = date.today()
first_day = date(today.year, today.month, 1)
last_day_prev_month = first_day - timedelta(days=1)
process_date = last_day_prev_month.strftime("%Y-%m-%d")

# 2. Union both sources in one go
kxud_salesforce_data = (
    spark.table("kes_clicks")
        .filter(
            (F.col("sourcename").isin("CKit KM Archive", "Salesforce KM")) &
            (F.length("domain") > 0) &
            (F.col("domain") != "Testing Admin Only") &
            (~F.col("domain").contains(",")) &
            (F.to_date("datetime") <= F.lit(process_date))
        )
        .withColumn(
            "domain",
            F.when(F.col("domain") == "SO Admin", "SO Operating Enablement")
             .otherwise(F.col("domain"))
        )
        .withColumn("date", F.lit(process_date))
        .select("date", "domain", F.countDistinct("clickid").alias("view_count"))
        .groupBy("date", "domain").agg(F.sum("view_count").alias("view_count"))
    .unionByName(
        spark.table("kes_clicks_salesforce")
            .filter(
                (F.length("domain") > 0) &
                (F.col("domain") != "Testing Admin Only") &
                (~F.col("domain").contains(",")) &
                (F.to_date("datetime") <= F.lit(process_date))
            )
            .withColumn(
                "domain",
                F.when(F.col("domain") == "SO Admin", "SO Operating Enablement")
                 .otherwise(F.col("domain"))
            )
            .withColumn("date", F.lit(process_date))
            .select("date", "domain", F.countDistinct("clickid").alias("view_count"))
            .groupBy("date", "domain").agg(F.sum("view_count").alias("view_count"))
    )
    .groupBy("date", "domain")
    .agg(F.sum("view_count").alias("view_count"))
)
