import logging, json
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    length,
    date_sub,
    current_date,
    next_day,
    countDistinct,
    when,
    expr,
    date_format,
    row_number,
    sum as spark_sum,
    max
)
from pyspark.sql import types as T
from pyspark.sql.window import Window
try:
    from ingestion_artifact import IngestionArtifact
    from databricks_utils import write_table
except Exception as e:
    logging.warning(str(e))
    sys.path.append(os.path.abspath('..'))
    from ingestion_artifact import IngestionArtifact
    from databricks_utils import write_table

# Initialize Spark session
def main(*args, **kwargs):
    logging.info("Beginning the transformation for kcs_knowlodge_domain_utlilization_summary_stage")
    spark = SparkSession.builder.getOrCreate()
    target_table_catalog = kwargs.get("target_catalog_name")
    target_table_schema = kwargs.get("target_schema_name")
    target_table_table = kwargs.get("target_table_name")
    qualified_target_table_name = f"{target_table_catalog}.{target_table_schema}.{target_table_table}"
    source_table_catalog = kwargs.get("source_catalog_name")
    source_table_schema = kwargs.get("source_schema_name")
    source_table_table = kwargs.get("source_table_name")
    qualified_source_schema = f"{source_table_catalog}.{source_table_schema}"
    load_type = kwargs.get('load_type')
 
# Date range    
    df_dates = spark.range(1).select(date_sub(next_day(current_date(), "Sun"), 14).alias("start_date"), date_sub(next_day(current_date(), "sat"), 7).alias("end_date"))
    start_date,end_date = df_dates.first()

    logging.info("Loading source tables")

# load table
    kcs_sf_knowledge_kav = spark.table(f"{qualified_source_schema}.kcs_sf_knowledge_kav")
    kcs_knowledge_link = spark.table(f"{qualified_source_schema}.kcs_knowledge_link")
    kcs_clicks = spark.table(f"{qualified_source_schema}.kcs_clicks")
    kcs_clicks_salesforce = spark.table(f"{qualified_source_schema}.kcs_clicks_salesforce")
    kcs_acrolinx_score = spark.table(f"{qualified_source_schema}.kcs_acrolinx_score")
    kcs_knowledge_flag = spark.table(f"{qualified_source_schema}.kcs_knowledge_flag")

    logging.info("Preparing inital Dataframes")

    logging.info("Preparing kcs_clicks_df")

    kcs_clicks_df = kcs_clicks.filter(
        (col("datetime").cast("date")).between(start_date, end_date) & 
        (col("source_name").isin("CKit KM Archive","Salesforce KH")) & 
        (length(col("domain"))>0) & 
        (col("domain") != "Testing Admin Only") & 
        (~col("domain").contains(","))
    ).select(
        col("datetime").cast("date").alias("date"),
        col("domain"),
        col("article_number"),
        col("document_title"),
        col("article_record_type").alias("record_type_description"),
        col("governance")
    )

    logging.info("Preparing kcs_clicks_salesforce_df")

    kcs_clicks_salesforce_df = (
        kcs_clicks_salesforce.filter(
            (col("datetime").cast("date").between(start_date, end_date)) & 
            (length(col("domain"))>0) & 
            (col("domain") != "Testing Admin Only") &
            (~col("domain").contains(","))
        ).select(
            col("datetime").cast("date").alias("date"),
            col("domain"),
            col("article_number").cast("string"),
            col("document_title"),
            col("article_record_type").alias("record_type_description"),
            col("governance")
        )
    )
    logging.info("Preparing kcs_sf_knowledge_kav_df")
    kcs_sf_knowledge_kav_df = (
        kcs_sf_knowledge_kav.alias("a")
        .join(kcs_knowledge_link.alias("b"), col("a.id") == col("b.KNOWLEDGE__C"), "inner")
        .filter(
            (col("LINK_DATE__C").cast("date").between(start_date, end_date)) & 
            (length(col("a.domain__c"))>0) & 
            (col("a.domain__c") != "Testing Admin Only") &
            (~col("a.domain__c").contains(",")) 
        ).select(
            col("b.LINK_DATE__C").cast("date"),
            col("domain__c").alias("domain"),
            # when(col("a.articlenumber" == "1"), col("a.articlenumber").cast("int")) \
            #     .otherwise(None).alias("article_number"),
            col("a.articlenumber").cast("string").alias("article_number"),
            col("title").alias("document_title"),
            when(col("a.RECORDTYPEID")=="0124P000000bg6GQAQ", "Alert")
            .when(col("a.RECORDTYPEID")=="0124P000000bg6HQAQ", "Issue")
            .when(col("RECORDTYPEID")=="0124P000000bg6IQAQ","Question")
            .when(col("RECORDTYPEID")=="0124P000000bg6JQAQ","Reference")
            .alias("record_type_desc"),
            col("a.GOVERNANCE__C").alias("governance")
        )
    )
    logging.info("Preparing kcs_knowledge_flag_df")
    kcs_knowledge_flag_df = (
    kcs_knowledge_flag.filter(
            (col("SUBMITTED_DATE_TIME__C").cast("date").between(start_date, end_date)) &
            (col("KNOWLEDGE_DOMAIN__C")>0) & 
            (col("KNOWLEDGE_DOMAIN__C") != "Testing Admin Only") & 
            (~col("KNOWLEDGE_DOMAIN__C").contains(",")
        )).select(
            col("SUBMITTED_DATE_TIME__C").cast("date").alias("date"),
            col("KNOWLEDGE_DOMAIN__C").alias("domain"),
            col("ARTICLE_NUMBER__C").alias("article_number"),
            col("TITLE__C").alias("document_title"),
            col("ARTICLE_RECORD_TYPE__C").alias("record_type_description"),
            col("GOVERNANCE__C").alias("governance")
        )
    )

    kcs_knowledge_domain_utilization_base = kcs_clicks_df.union(kcs_clicks_salesforce_df).union(kcs_knowledge_flag_df).union(kcs_knowledge_flag_df).union(kcs_sf_knowledge_kav_df).distinct()

    logging.info("Calculating and updating article_view_counts")
# Calculating and updating article_view_counts
    kxud_salesforce_data_1 = kcs_clicks.filter(
        (col("datetime").cast("date").between(start_date, end_date)) & 
        (col("source_name").isin('CKit KM Archive', 'Salesforce KM')) &
        (length(col("domain"))>0) &
        (col("domain") != "'Testing Admin Only") & 
        (~col("domain").contains(","))
    ).groupBy(
        (col("datetime").cast("date").alias("date")),
        (col("domain")),
        (col("article_number")),
        (col("document_title")),
        (col("article_record_type").alias("record_type_description")),
        (col("governance"))
    ).agg(
        countDistinct("click_Id").alias("article_view_counts")
    )
    display(kxud_salesforce_data_1)
    logging.info("Calculating kxud_salesforce_data_2")
    kxud_salesforce_data_2 = kcs_clicks_salesforce.filter(
        (col("datetime").cast("date").between(start_date, end_date)) & 
        (col("source_name").isin('CKit KM Archive', 'Salesforce KM')) &
        (length(col("domain"))>0) &
        (col("domain") != "'Testing Admin Only") & 
        (~col("domain").contains(","))
    ).groupBy(
        (col("datetime").cast("date").alias("date")),
        (col("domain")),
        (col("article_number")),
        (col("document_title")),
        (col("article_record_type").alias("record_type_description")),
        (col("governance"))
    ).agg(
        countDistinct("click_Id").alias("article_view_counts")
    )
    
    logging.info("unioning kxud_salesforce_data_combined")
    kxud_salesforce_data_combined = kxud_salesforce_data_1.union(kxud_salesforce_data_2)
#   date, domain, article_number, document_titile, record_type_description, governance
    kxud_salesforce_data = kxud_salesforce_data_combined.groupBy("date", "domain", "article_number", "document_title", "record_type_description", "governance").agg(spark_sum("article_view_counts").alias("article_view_counts"))
    display(kxud_salesforce_data)

# need to complete colum updates
    
    logging.info("forming kcs_knowledge_domain_utilization_intm_joined_1")
    kcs_knowledge_domain_utilization_intm_joined_1 = (
        kcs_knowledge_domain_utilization_base.alias("a")
            .join(
                kxud_salesforce_data.alias("b"),
                (col("a.date") == col("b.date")) &
                (col("a.domain") == col("b.domain")) &
                (col("a.article_number") == col("b.article_number")) &
                (col("a.document_title") == col("b.document_title")) &
                (col("a.record_type_description") == col("b.record_type_description")) &
                (col("a.governance") == col("b.governance")),
                "inner",
            )
            .select(
                col("a.date").alias("date"),
                col("a.domain").alias("domain"),
                col("a.article_number").alias("article_number"),
                col("a.document_title").alias("document_title"),
                col("a.record_type_description").alias("record_type_description"),
                col("a.governance").alias("governance"),
                col("b.article_view_counts").alias("article_view_counts")
            )
    )
                                                
    logging.info("forming kcs_knowledge_domain_utilization_intm_1")
    display(kcs_knowledge_domain_utilization_intm_joined_1)
                                                
    # kcs_knowledge_domain_utilization_intm_1 = kcs_knowledge_domain_utilization_intm_joined_1 \
    #                                         .withColumn("article_view_counts", when(col("kxud_salesforce_data.article_view_counts").isNotNull(), col("kxud_salesforce_data.article_view_counts"))
    #                                                     .otherwise(col("kcs_knowledge_domain_utilization_base.article_view_counts"))
    #                                             ).select("kcs_knowledge_domain_utilization_base.*")
                                            
    kcs_knowledge_domain_utilization_intm_1 = kcs_knowledge_domain_utilization_intm_joined_1
    # .select(
    #     col("a.*"), # Selects all columns from the base DataFrame using its alias
    #     col("b.article_view_counts")
    # )
    
    display(kcs_knowledge_domain_utilization_intm_1)
    
    logging.info(" Calculating and updating Use_Counts,Reference_Counts,article_created_date,product_line,shared_with,validation_status,link_date,last_modified_date")
# Calculating and updating Use_Counts,Reference_Counts,article_created_date,product_line,shared_with,validation_status,link_date,last_modified_date
    kav_data = kcs_sf_knowledge_kav.alias("a").join(
    kcs_knowledge_link.alias("b"),
    col("a.id") == col("b.KNOWLEDGE__C"),
    "inner"
    ).filter(
        (date_format(col("b.LINK_DATE__C"), 'yyyy-MM-dd').between(start_date, end_date)) &
        (col("a.domain__c").isNotNull()) &
        (col("a.domain__c") != 'Testing Admin Only') &
        (~col("a.domain__c").contains(","))
    ).groupBy(
        date_format(col("b.LINK_DATE__C"), 'yyyy-MM-dd').alias("link_date"),
        col("a.domain__c").alias("domain"),
        # when(col("a.articlenumber").cast("string").rlike("^[0-9]+$"), col("a.articlenumber").cast("int")).otherwise(col("a.articlenumber")).cast("string").alias("article_number"),
        col("a.articlenumber").cast("string").alias("article_number"),
        col("a.title").alias("document_title"),
        when(col("a.RECORDTYPEID") == '0124P000000bg6GQAQ', 'Alert')
        .when(col("a.RECORDTYPEID") == '0124P000000bg6HQAQ', 'Issue')
        .when(col("a.RECORDTYPEID") == '0124P000000bg6IQAQ', 'Question')
        .when(col("a.RECORDTYPEID") == '0124P000000bg6JQAQ', 'Reference').alias("record_type_description"),
        col("a.GOVERNANCE__C").alias("governance")
    ).agg(
        max(date_format(col("a.articlecreateddate"), 'yyyy-MM-dd')).alias("article_created_date"),
        spark_sum(when(col("b.type__c") == 'Use', 1).otherwise(0)).alias("Use_Counts"),
        spark_sum(when(col("b.type__c") == 'Reference', 1).otherwise(0)).alias("Reference_Counts"),
        max(col("a.product_line__c")).alias("product_line"),
        max(col("a.share_with__c")).alias("shared_with"),
        max(col("a.VALIDATIONSTATUS")).alias("validation_status"),
        max(date_format(col("a.lastmodifieddate"), 'yyyy-MM-dd')).alias("last_modified_date"),
        expr("0").alias("acrolinx_score")    
    ).alias("k")

# need to complete column updates
    kcs_knowledge_domain_utilization_intm_joined_2 = (
        kcs_knowledge_domain_utilization_intm_1.alias("a")
            .join(
                kav_data.alias("b"),
                (col("a.date") == col("b.link_date")) &
                (col("a.domain") == col("b.domain")) &
                (col("a.article_number") == col("b.article_number")) &
                (col("a.document_title") == col("b.document_title")) &
                (col("a.record_type_description") == col("b.record_type_description")) &
                (col("a.governance") == col("b.governance")),
                "inner",
            ).select(
                col("a.date").alias("date"),
                col("a.domain").alias("domain"),
                col("a.article_number").alias("article_number"),
                col("a.document_title").alias("document_title"),
                col("a.record_type_description").alias("record_type_description"),
                col("a.governance").alias("governance"),
                col("a.article_view_counts").alias("article_view_counts"),
                col("b.Use_Counts").alias("Use_Counts"),
                col("b.Reference_Counts").alias("Reference_Counts"),
                col("b.article_created_date").alias("article_created_date"),
                col("b.product_line").alias("product_line"),
                col("b.shared_with").alias("shared_with"),
                col("b.validation_status").alias("validation_status"),
                col("b.link_date").alias("link_date"),
                col("b.last_modified_date").alias("last_modified_date")
            )
    )
    kcs_knowledge_domain_utilization_intm_2 = kcs_knowledge_domain_utilization_intm_joined_2
    # kcs_knowledge_domain_utilization_intm_2 = kcs_knowledge_domain_utilization_intm_joined_2.alias("a").select( \
    #                                             col("kcs_knowledge_domain_utilization_intm_1.*"),
    #                                             when(col("k.Use_Counts").isNotNull(), col("k.Use_Counts")).otherwise(col("a.Use_Counts")).alias("Use_Counts"),
    #                                             when(col("k.Reference_Counts").isNotNull(), col("k.Reference_Counts")).otherwise(col("a.Reference_Counts")).alias("Reference_Counts"),
    #                                             when(col("k.article_created_date").isNotNull(), col("k.article_created_date")).otherwise(col("a.article_created_date")).alias("article_created_date"),
    #                                             when(col("k.product_line").isNotNull(), col("k.product_line")).otherwise(col("a.product_line")).alias("product_line"),
    #                                             when(col("k.shared_with").isNotNull(), col("k.shared_with")).otherwise(col("a.shared_with")).alias("shared_with"),
    #                                             when(col("k.validation_status").isNotNull(), col("k.validation_status")).otherwise(col("a.validation_status")).alias("validation_status"),
    #                                             when(col("k.link_date").isNotNull(), col("k.link_date")).otherwise(col("a.link_date")).alias("link_date"),
    #                                             when(col("k.last_modified_date").isNotNull(), col("k.last_modified_date")).otherwise(col("a.last_modified_date")).alias("last_modified_date")
    # )

    logging.info("Calculating and updating article_created_date,product_line,shared_with,validation_status")
# Calculating and updating article_created_date,product_line,shared_with,validation_status
    flag_data = (kcs_knowledge_flag
        .filter((col("SUBMITTED_DATE_TIME__C").cast("date").between(start_date, end_date)) &
                (length(col("KNOWLEDGE_DOMAIN__C")) > 0) &
                (col("KNOWLEDGE_DOMAIN__C") != 'Testing Admin Only') &
                (~col("KNOWLEDGE_DOMAIN__C").contains(',')))
        .groupBy(col("SUBMITTED_DATE_TIME__C").cast("date").alias("submitted_date"),
                col("KNOWLEDGE_DOMAIN__C").alias("domain"),
                # when(col("ARTICLE_NUMBER__C" == "1"), col("ARTICLE_NUMBER__C").cast("int"))
                # .otherwise(col("ARTICLE_NUMBER__C")).cast("string").alias("article_number"),
                col("ARTICLE_NUMBER__C").cast("string").alias("article_number"),
                col("TITLE__C").alias("document_title"),
                col("ARTICLE_RECORD_TYPE__C").alias("record_type_description"),
                col("GOVERNANCE__C").alias("governance"))
        .agg(spark_sum(when(col("status__c").isin('Draft', 'Withdrawn'), 0).otherwise(1)).alias("created_flag_count")
        )
        
    )
    logging.info("kcs_knowledge_domain_utilization_intm_3")
    kcs_knowledge_domain_utilization_intm_3 = (kcs_knowledge_domain_utilization_intm_2.alias("a")
        .join(flag_data.alias("b"),
            (col("a.date") == col("b.submitted_date")) &
            (col("a.domain") == col("b.domain")) &
            (col("a.article_number") == col("b.article_number")) &
            (col("a.document_title") == col("b.document_title")) &
            (col("a.record_type_description") == col("b.record_type_description")) &
            (col("a.governance") == col("b.governance")),
            "inner")
        .select(
            col("a.date").alias("date"),
            col("a.domain").alias("domain"),
            col("a.article_number").alias("article_number"),
            col("a.document_title").alias("document_title"),
            col("a.record_type_description").alias("record_type_description"),
            col("a.governance").alias("governance"),
            col("a.article_view_counts").alias("article_view_counts"),
            col("a.Use_Counts").alias("Use_Counts"),
            col("a.Reference_Counts").alias("Reference_Counts"),
            col("a.article_created_date").alias("article_created_date"),
            col("a.product_line").alias("product_line"),
            col("a.shared_with").alias("shared_with"),
            col("a.validation_status").alias("validation_status"),
            col("a.link_date").alias("link_date"),
            col("a.last_modified_date").alias("last_modified_date"),
            col("b.created_flag_count").alias("created_flag_count")
        )
    )

    kcs_knowledge_domain_utilization_intm_4 = kcs_knowledge_domain_utilization_intm_3 
    # \
    #                                             .withColumn("created_flag_counts", when(col("flag_data.created_flag_counts").isNotNull(), col("flag_data.created_flag_counts"))
    #                                                         .otherwise(col("kcs_knowledge_domain_utilization_intm_3.created_flag_counts"))
    #                                             ).select("kcs_knowledge_domain_utilization_intm_3.*")

    # Second update equivalent
    logging.info("Kav_link_data")
    Kav_link_data = (kcs_sf_knowledge_kav.alias("a")
        .join(kcs_knowledge_link.alias("b"), col("a.id") == col("b.KNOWLEDGE__C"), "left")
        .filter(
                (length(col("a.domain__c")) > 0) & 
                (col("a.domain__c") != "Testing Admin Only") &
                (col("a.domain__c") != "Testing Admin Only") &
                (~col("a.domain__c").contains(','))
                )
        .select(col("a.articlenumber").cast("string").alias("article_number"),
                # when(col("a.articlenumber)" == "1"), col("a.articlenumber").cast("int"))
                # .otherwise(col("a.articlenumber")).cast("string").alias("article_number"),
                col("a.articlecreateddate").cast("date").alias("article_created_date"),
                col("a.product_line__c").alias("product_line"),
                col("a.share_with__c").alis("shared_with"),
                col("a.VALIDATIONSTATUS").alias("VALIDATIONSTATUS"),
                col("a.lastmodifieddate").cast("date").alias("last_modified_date"),
                row_number().over(Window.partitionBy(when(col("a.articlenumber") == "1",
                                                          col("a.articlenumber").cast("int"))
                .otherwise(col("a.articlenumber")).cast("string")).orderBy(col("b.LINK_DATE__C").cast("date").desc())).alias("rnk"))
    )
    logging.info("Link date filtering")
    Kav_link_data = Kav_link_data.filter(col("rnk") == 1).select(
        "article_number", "article_created_date", "product_line", "shared_with","VALIDATIONSTATUS", "last_modified_date"
        )
    logging.info("kcs_knowledge_domain_utilization_intm_4")
    kcs_knowledge_domain_utilization_stg = (kcs_knowledge_domain_utilization_intm_4.alias("a")
            .join(Kav_link_data.alias("b"), col("a.article_number") == col("b.article_number"), "inner")
            .select("a.*",
                    when(col("a.article_created_date").isNull(), col("b.article_created_date")).otherwise(col("a.article_created_date")).alias("article_created_date"),
                    when(col("a.product_line").isNull(), col("b.product_line")).otherwise(col("a.product_line")).alias("product_line"),
                    when(col("a.shared_with").isNull(), col("b.shared_with")).otherwise(col("a.shared_with")).alias("shared_with"),
                    when(col("a.validation_status").isNull(), col("b.VALIDATIONSTATUS")).otherwise(col("a.validation_status")).alias("validation_status"),
                    when(col("a.last_modified_date").isNull(), col("b.last_modified_date")).otherwise(col("a.last_modified_date")).alias("last_modified_date"))
    )
    logging.info("kkud_stg")
    kkud_stg = ( 
                kcs_acrolinx_score.withColumn("date_score_changed", col('date_score_changed__c').cast('date'))\
                .withColumn("acrolinx_score", col("acrolinx_score__c"))\
                .withColumn("acrolinx_score__c", col("acrolinx_score__c").cast("int"))
    )
    logging.info("window partition")
    window = Window.partitionBy("article_number__c", "date_score_changed__c".cast("date")).orderBy("date_score_changed__c").desc()
    kcs_acrolinx_score_data = kkud_stg.withColumn("rnk", row_number().over(window)).filter("rnk"==1)
    final_df = kcs_knowledge_domain_utilization_stg.alias("a").join(kcs_acrolinx_score_data.alias("b")
        (col("a.article_number")==col("b.article_number")) &
        (col("a.date")==col("b.date_score_changed")),
        "left"
    ).filter(
        col("a.overnance".isin('Experience-Based', 'Rule-Based'))
    )


    logging.info(f"Executing the filters and saving the data into delta table: {qualified_target_table_name}")
    target_artifact = IngestionArtifact(
            spark,
            target_table_catalog,
            target_table_schema,
            target_table_table
        )
    perms = kwargs.get('target_table_permissions',{})
    perms = json.loads(perms) if isinstance(perms, str) else perms
    write_table(
            spark=spark,
            artifact=target_artifact,
            data_df=final_df,
            source_system = kwargs.get('source_system_name'),
            conditions = "",
            permissions=perms,
            mode=load_type
        )
    logging.info("Transformation complete")
