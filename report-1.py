process_date = "2025-06-30"

bookmark_df = (
    spark.table("EDW_BASE_O.KCS_BOOKMARK_VW").alias("A")
    .join(
        spark.table("EDW_BASE_O.KCS_KX_ARTICLE_VERSION_VW").alias("B"),
        F.col("A.KNOWLEDGE_ARTICLE_C") == F.col("B.ARTICLE_VERSION_ID_C"),
        "inner"
    )
    .filter(
        (F.to_date("A.CREATEDDATE") <= F.lit(process_date)) &
        (F.col("B.ARTICLE_DOMAIN_C") != "Testing Admin Only")
    )
    .withColumn(
        "domain",
        F.when(F.col("B.ARTICLE_DOMAIN_C") == "SO Admin", "SO Operating Enablement")
         .otherwise(F.col("B.ARTICLE_DOMAIN_C"))
    )
    .withColumn("date", F.lit(process_date))
    .groupBy("date", "domain")
    .agg(F.countDistinct("A.ID").alias("bookmark_count"))
)
