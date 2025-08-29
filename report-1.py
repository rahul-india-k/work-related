process_date = "2025-06-30"

flag_count_df = (
    spark.table("kcs_knowledge_flag")
    .filter(
        (F.length("KNOWLEDGE_DOMAIN_C") > 0) &
        (F.col("KNOWLEDGE_DOMAIN_C") != "Testing Admin Only") &
        (~F.col("KNOWLEDGE_DOMAIN_C").contains(",")) &
        (F.to_date("SUBMITTED_DATE_TIME_C") <= F.lit(process_date))
    )
    .withColumn(
        "domain",
        F.when(F.col("KNOWLEDGE_DOMAIN_C") == "SO Admin", "SO Operating Enablement")
         .otherwise(F.col("KNOWLEDGE_DOMAIN_C"))
    )
    .withColumn("date", F.lit(process_date))
    .groupBy("date", "domain")
    .agg(
        F.sum(
            F.when(F.col("status_c").isin("Draft", "Withdrawn"), F.lit(0)).otherwise(F.lit(1))
        ).alias("flag_count")
    )
)
