"""
Silver Azure DevOps User Stories transformation pipeline.

Title
-----
azure_devops_transformations_silver.py

Description
-----------
Builds the silver layer for Azure DevOps User Stories. Parses the bronze raw
payload, applies CDC into a current table, and creates the final silver table
with expanded business fields.

Dependencies
------------
azure_devops_ingestion_bronze.py
"""

# COMMAND ----------
# DBTITLE 1, Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    element_at,
    expr,
    parse_json,
    regexp_replace,
    split,
    struct,
    trim,
    when,
)

BRONZE_SCHEMA = spark.conf.get("DEFAULT_BRONZE_ADO_SCHEMA")

dp.create_streaming_table(
    name="azure_devops_user_stories_current",
    table_properties={
        "delta.feature.variantType-preview": "supported"
    },
)

dp.create_auto_cdc_flow(
    target="azure_devops_user_stories_current",
    source="azure_devops_user_stories_parsed",
    keys=["id"],
    sequence_by=struct(col("rev"), col("extraction_timestamp")),
    stored_as_scd_type=1,
    name="apply_azure_devops_user_story_changes_into_current",
)


@dp.table(
    name="azure_devops_user_stories_parsed",
    private=True,
    table_properties={
        "delta.feature.variantType-preview": "supported"
    },
    comment="Intermediate parsed Azure DevOps User Stories table.",
)
def azure_devops_user_stories_parsed():
    """Parse the bronze raw payload."""
    parsed = (
        spark.readStream.table(f"{BRONZE_SCHEMA}.azure_devops_user_stories")
        .withColumn("raw_data", parse_json("raw_data"))
        .withColumn("id", col("id").cast("string"))
        .withColumn("rev", col("rev").cast("int"))
        .select(
            col("id"),
            col("rev"),
            col("title"),
            col("url"),
            col("org"),
            col("project"),
            expr("CAST(raw_data:fields['System.WorkItemType'] AS STRING)").alias("work_item_type"),
            expr("CAST(raw_data:fields['System.State'] AS STRING)").alias("state"),
            expr("CAST(raw_data:fields['System.Reason'] AS STRING)").alias("reason"),
            expr("CAST(raw_data:fields['System.AreaPath'] AS STRING)").alias("area_path"),
            expr("CAST(raw_data:fields['System.IterationPath'] AS STRING)").alias("iteration_path"),
            expr("CAST(raw_data:fields['System.CreatedDate'] AS TIMESTAMP)").alias("created_date"),
            expr("CAST(raw_data:fields['System.ChangedDate'] AS TIMESTAMP)").alias("changed_date"),
            expr("CAST(raw_data:fields['System.CreatedBy'].displayName AS STRING)").alias("created_by"),
            expr("CAST(raw_data:fields['System.ChangedBy'].displayName AS STRING)").alias("changed_by"),
            expr("CAST(raw_data:fields['System.AssignedTo'].displayName AS STRING)").alias("assigned_to"),
            expr("CAST(raw_data:fields['System.Parent'] AS STRING)").alias("parent_id"),
            expr("CAST(raw_data:fields['System.Tags'] AS STRING)").alias("tags"),
            expr("CAST(raw_data:fields['System.Description'] AS STRING)").alias("description_html"),
            expr(
                "CAST(raw_data:fields['Microsoft.VSTS.Common.AcceptanceCriteria'] AS STRING)"
            ).alias("acceptance_criteria_html"),
            expr("CAST(raw_data:fields['System.BoardColumn'] AS STRING)").alias("board_column"),
            expr("CAST(raw_data:fields['System.BoardColumnDone'] AS BOOLEAN)").alias("board_column_done"),
            expr("CAST(raw_data:fields['Microsoft.VSTS.Common.Priority'] AS INT)").alias("priority"),
            expr("CAST(raw_data:fields['Microsoft.VSTS.Scheduling.StoryPoints'] AS INT)").alias("story_points"),
            expr("CAST(raw_data:fields['Microsoft.VSTS.Common.StateChangeDate'] AS TIMESTAMP)").alias("state_change_date"),
            expr("CAST(raw_data:fields['Microsoft.VSTS.Common.ActivatedDate'] AS TIMESTAMP)").alias("activated_date"),
            expr("CAST(raw_data:fields['Microsoft.VSTS.Common.ClosedDate'] AS TIMESTAMP)").alias("closed_date"),
            expr("CAST(raw_data:fields['Custom.TimeTrackingProject'] AS STRING)").alias("time_tracking_project"),
            expr("CAST(raw_data:fields['Custom.TimeTrackingProductID'] AS STRING)").alias("time_tracking_product_id"),
            expr("CAST(raw_data:fields['Custom.Client_ID'] AS STRING)").alias("client_id"),
            col("extraction_timestamp"),
            col("raw_data"),
        )
        .withColumn(
            "description",
            trim(regexp_replace(col("description_html"), "<[^>]+>", " "))
        )
        .withColumn(
            "acceptance_criteria",
            trim(regexp_replace(col("acceptance_criteria_html"), "<[^>]+>", " "))
        )
        .withColumn(
            "client",
            element_at(split(col("area_path"), r"\\"), -1),
        )
        .withColumn(
            "sprint_name",
            element_at(split(col("iteration_path"), r"\\"), -1),
        )
        .withColumn(
            "area_level_1",
            element_at(split(col("area_path"), r"\\"), 1),
        )
        .withColumn(
            "area_level_2",
            element_at(split(col("area_path"), r"\\"), 2),
        )
        .withColumn(
            "area_level_3",
            element_at(split(col("area_path"), r"\\"), 3),
        )
        .withColumn(
            "iteration_level_1",
            element_at(split(col("iteration_path"), r"\\"), 1),
        )
        .withColumn(
            "iteration_level_2",
            element_at(split(col("iteration_path"), r"\\"), 2),
        )
        .withColumn(
            "iteration_level_3",
            element_at(split(col("iteration_path"), r"\\"), 3),
        )
        .withColumn(
            "is_closed",
            when(
                trim(col("state")).isin("Closed", "Done", "Removed"),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "has_parent",
            when(col("parent_id").isNotNull(), True).otherwise(False),
        )
        .withColumn(
            "has_tags",
            when(
                col("tags").isNotNull() & (trim(col("tags")) != ""),
                True,
            ).otherwise(False),
        )
    )

    return parsed