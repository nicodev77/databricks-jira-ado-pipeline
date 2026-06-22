"""
Gold pipeline for mapped user stories.

Description
-----------
Maps client JIRA issues to internal Azure DevOps user stories,
creating a unified view across both systems.
"""

import solution_code  # noqa: F401

from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, transform, variant_get

SILVER_ADO_SCHEMA = spark.conf.get("DEFAULT_SILVER_ADO_SCHEMA")
SILVER_CLIENT_SCHEMA = spark.conf.get("DEFAULT_SILVER_CLIENT_SCHEMA")

MAPPED_USER_STORIES_SCHEMA = """
    jira_id STRING NOT NULL COMMENT 'Unique JIRA issue ID',
    jira_name STRING COMMENT 'Title of the work item in JIRA',
    jira_project_name STRING COMMENT 'JIRA project name',
    jira_issue_type STRING COMMENT 'Type: Epic, User Story, Task, or Bug',
    jira_status STRING COMMENT 'Current status in JIRA',
    jira_description STRING COMMENT 'Description of the work item',
    jira_reporter STRING COMMENT 'Person who created the work item',
    jira_assignee STRING COMMENT 'Person currently assigned',
    jira_priority STRING COMMENT 'Priority level',
    jira_story_points INT COMMENT 'Story points assigned in JIRA',
    jira_parent_id STRING COMMENT 'ID of the parent work item',
    jira_fix_versions ARRAY<STRING> COMMENT 'Release names this item is associated with',
    release_ids ARRAY<STRING> COMMENT 'JIRA fix version numeric IDs for joining with releases table',
    jira_labels ARRAY<STRING> COMMENT 'Labels assigned to the work item',
    jira_start_date STRING COMMENT 'Planned start date',
    jira_due_date STRING COMMENT 'Expected completion date',
    jira_created_date TIMESTAMP COMMENT 'Date the work item was created',
    jira_updated_date TIMESTAMP COMMENT 'Date the work item was last updated',
    jira_resolution STRING COMMENT 'Final resolution',
    jira_url STRING COMMENT 'Direct URL to the JIRA issue',
    jira_is_closed BOOLEAN COMMENT 'Whether the work item is closed',
    ado_story_id STRING COMMENT 'Corresponding Azure DevOps work item ID',
    ado_parent_id STRING COMMENT 'Parent ID in Azure DevOps',
    ado_story_points INT COMMENT 'Story points in Azure DevOps',
    ado_revision INT COMMENT 'Revision number — increments with each update',
    ado_name STRING COMMENT 'Title in Azure DevOps',
    ado_description STRING COMMENT 'Description in Azure DevOps',
    ado_acceptance_criteria STRING COMMENT 'Acceptance criteria',
    ado_board_column STRING COMMENT 'Current board column — PRIMARY source for unified state',
    ado_board_done BOOLEAN COMMENT 'Whether marked as done at board level',
    ado_priority INT COMMENT 'Priority in Azure DevOps',
    ado_project STRING COMMENT 'Azure DevOps project',
    ado_status STRING COMMENT 'Status in Azure DevOps',
    ado_is_closed BOOLEAN COMMENT 'Whether closed in Azure DevOps',
    ado_reason STRING COMMENT 'Reason for most recent status change',
    ado_iteration STRING COMMENT 'Sprint or release iteration',
    ado_tags STRING COMMENT 'Internal tags',
    internal_area STRING COMMENT 'Internal area path',
    internal_client STRING COMMENT 'Client this work item is associated with',
    sprint_name STRING COMMENT 'Sprint name',
    time_tracking_project STRING COMMENT 'Time tracking project',
    time_tracking_product_id STRING COMMENT 'Time tracking subtask'
"""


@dp.expect(
    "Missing matching Azure DevOps User Story",
    "ado_story_id IS NOT NULL"
)
@dp.materialized_view(
    name="mapped_user_stories",
    comment="Mapping between client JIRA User Stories and internal Azure DevOps User Stories",
    schema=MAPPED_USER_STORIES_SCHEMA,
)
def mapped_user_stories():
    """
    Gold materialized view — maps client JIRA issues to internal ADO user stories.
    Core business logic of the pipeline.

    Join decisions:
    - LEFT JOIN: preserves all Jira issues even without an ADO match.
      INNER JOIN would silently drop unmapped stories.
    - trim() on both keys: both systems had whitespace inconsistencies.
      Without trim(), valid matches failed silently.
    - join key: jira_id = client_id (ADO custom field storing the Jira ID)

    @dp.expect monitors unmapped stories without blocking the pipeline.
    Recalculated on every pipeline run.
    """
    jira_df = spark.read.table(f"{SILVER_CLIENT_SCHEMA}.jira_issues_current").alias("jira")
    ado_df = spark.read.table(
        f"{SILVER_ADO_SCHEMA}.azure_devops_user_stories_current"
    ).filter(col("state") != "Removed").alias("ado")

    mapped = (
        jira_df.join(
            ado_df,
            trim(col("jira.jira_id")) == trim(col("ado.client_id")),
            "left",
        )
        .select(
            col("jira.jira_id").alias("jira_id"),
            col("jira.jira_name").alias("jira_name"),
            col("jira.project_name").alias("jira_project_name"),
            col("jira.issue_type").alias("jira_issue_type"),
            col("jira.status").alias("jira_status"),
            col("jira.description").alias("jira_description"),
            col("jira.reporter").alias("jira_reporter"),
            col("jira.assignee").alias("jira_assignee"),
            col("jira.priority").alias("jira_priority"),
            col("jira.story_points").alias("jira_story_points"),
            col("jira.parent_jira_id").alias("jira_parent_id"),
            transform(
                col("jira.fix_versions"),
                lambda x: variant_get(x, "$.name", "string")
            ).alias("jira_fix_versions"),
            col("jira.fix_version_ids").alias("release_ids"),
            col("jira.labels").alias("jira_labels"),
            col("jira.start_date").alias("jira_start_date"),
            col("jira.due_date").alias("jira_due_date"),
            col("jira.created_date").alias("jira_created_date"),
            col("jira.updated_date").alias("jira_updated_date"),
            col("jira.resolution").alias("jira_resolution"),
            col("jira.url").alias("jira_url"),
            col("jira.is_closed").alias("jira_is_closed"),
            col("ado.id").alias("ado_story_id"),
            col("ado.parent_id").alias("ado_parent_id"),
            col("ado.story_points").alias("ado_story_points"),
            col("ado.rev").alias("ado_revision"),
            col("ado.title").alias("ado_name"),
            col("ado.description").alias("ado_description"),
            col("ado.acceptance_criteria").alias("ado_acceptance_criteria"),
            col("ado.board_column").alias("ado_board_column"),
            col("ado.board_column_done").alias("ado_board_done"),
            col("ado.priority").alias("ado_priority"),
            col("ado.project").alias("ado_project"),
            col("ado.state").alias("ado_status"),
            col("ado.is_closed").alias("ado_is_closed"),
            col("ado.reason").alias("ado_reason"),
            col("ado.iteration_path").alias("ado_iteration"),
            col("ado.tags").alias("ado_tags"),
            col("ado.area_path").alias("internal_area"),
            col("ado.client").alias("internal_client"),
            col("ado.sprint_name").alias("sprint_name"),
            col("ado.time_tracking_project").alias("time_tracking_project"),
            col("ado.time_tracking_product_id").alias("time_tracking_product_id"),
        )
    )
    return mapped