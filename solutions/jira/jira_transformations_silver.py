"""
Silver JIRA Issues transformation pipeline.

Description
-----------
Builds the silver layer for JIRA Issues. Parses the bronze raw
payload, applies CDC into a current table, and creates the final silver
table with expanded business fields.

Dependencies
------------
jira_ingestion_bronze.py
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, coalesce, parse_json, struct,
    transform, trim, variant_get, when,
)

BRONZE_SCHEMA = spark.conf.get("DEFAULT_BRONZE_CLIENT_SCHEMA")


dp.create_streaming_table(
    name="jira_issues_current",
    table_properties={"delta.feature.variantType-preview": "supported"},
)

dp.create_auto_cdc_flow(
    target="jira_issues_current",
    source="jira_issues_parsed",
    keys=["jira_id"],
    sequence_by=struct(col("updated_date"), col("extraction_timestamp")),
    stored_as_scd_type=1,
    name="apply_jira_issue_changes_into_current",
)


@dp.table(
    name="jira_issues_parsed",
    private=True,
    table_properties={"delta.feature.variantType-preview": "supported"},
    comment="Intermediate parsed JIRA Issues table.",
)
def jira_issues_parsed():
    """
    Private intermediate DLT table — parses Bronze raw JSON into structured columns.
    Not exposed as a final output (private=True).
    Uses variant_get instead of from_json because Jira's schema varies
    across issue types (Epic vs Bug vs Story) — variant_get is resilient
    to API schema changes without requiring a predefined schema.
    Adds is_closed and has_parent boolean flags once here so Gold
    queries don't need to repeat this logic.
    Feeds into the CDC flow that upserts jira_issues_current.
    """
    parsed = (
        spark.readStream.table(f"{BRONZE_SCHEMA}.jira_issues")
        .withColumn("raw_data", parse_json("raw_data"))
        .select(
            col("jira_internal_id"),
            col("key").alias("jira_id"),
            col("summary").alias("jira_name"),
            col("project").alias("project_key"),
            coalesce(
                variant_get(col("raw_data"), "$.fields.project.name", "string"),
                col("project"),
            ).alias("project_name"),
            col("issue_type"),
            variant_get(col("raw_data"), "$.fields.status.name", "string").alias("status"),
            variant_get(col("raw_data"), "$.fields.description", "string").alias("description"),
            variant_get(col("raw_data"), "$.fields.reporter.displayName", "string").alias("reporter"),
            variant_get(col("raw_data"), "$.fields.assignee.displayName", "string").alias("assignee"),
            variant_get(col("raw_data"), "$.fields.priority.name", "string").alias("priority"),
            variant_get(col("raw_data"), "$.fields.customfield_10022", "int").alias("story_points"),
            variant_get(col("raw_data"), "$.fields.parent.key", "string").alias("parent_jira_id"),
            variant_get(col("raw_data"), "$.fields.fixVersions", "array<variant>").alias("fix_versions"),
            transform(
                variant_get(col("raw_data"), "$.fields.fixVersions", "array<variant>"),
                lambda x: variant_get(x, "$.id", "string")
            ).alias("fix_version_ids"),
            variant_get(col("raw_data"), "$.fields.labels", "array<string>").alias("labels"),
            variant_get(col("raw_data"), "$.fields.customfield_10015", "string").alias("start_date"),
            variant_get(col("raw_data"), "$.fields.duedate", "string").alias("due_date"),
            variant_get(col("raw_data"), "$.fields.created", "timestamp").alias("created_date"),
            variant_get(col("raw_data"), "$.fields.updated", "timestamp").alias("updated_date"),
            variant_get(col("raw_data"), "$.fields.resolution.name", "string").alias("resolution"),
            col("url"),
            col("extraction_timestamp"),
        )
        .withColumn(
            "is_closed",
            when(trim(col("status")).isin("Closed", "Done", "Resolved"), True).otherwise(False),
        )
        .withColumn(
            "has_parent",
            when(col("parent_jira_id").isNotNull(), True).otherwise(False),
        )
    )
    return parsed