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

process_date = "2025-06-30"

headline_news_df = (
    spark.table("sf_kcs_headline_news")
    .filter(F.to_date("createddate") <= F.lit(process_date))
    .withColumn("domain", F.explode(F.split(F.col("VIEWABLE_BY_C"), ";")))
    .withColumn("date", F.lit(process_date))
    .groupBy("date", "domain")
    .agg(F.countDistinct("id").alias("headline_news_count"))
)


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

