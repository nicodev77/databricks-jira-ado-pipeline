"""
Gold releases pipeline.

Description
-----------
One materialized view built on top of mapped_user_stories:
  - releases: unified release table with release_id as primary key,
              combining workload and progress metrics per release.
"""

import solution_code  # noqa: F401

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from solution_code.utils import unified_state

MAPPED_USER_STORIES_TABLE = "mapped_user_stories"

RELEASES_SCHEMA = """
    release_id                        STRING    NOT NULL   COMMENT 'JIRA fix version numeric ID. Primary key.',
    release_name                      STRING               COMMENT 'Name of the JIRA fix version / release',
    story_point_total                 DOUBLE               COMMENT 'Sum of story points. ADO preferred, JIRA as fallback via COALESCE',
    amount_of_work_items              BIGINT    NOT NULL   COMMENT 'Count of all work items assigned to this release',
    work_items_without_story_points   BIGINT    NOT NULL   COMMENT 'Number of work items with no story points in either system',
    total_work                        BIGINT    NOT NULL   COMMENT 'Total work items. Must equal sum of all state columns',
    unstarted                         BIGINT    NOT NULL,
    in_progress                       BIGINT    NOT NULL,
    blocked                           BIGINT    NOT NULL,
    pending_review                    BIGINT    NOT NULL,
    merged_internally                 BIGINT    NOT NULL,
    merged_to_client                  BIGINT    NOT NULL,
    user_testing                      BIGINT    NOT NULL,
    done                              BIGINT    NOT NULL,
    removed                           BIGINT    NOT NULL,
    uncategorised                     BIGINT    NOT NULL   COMMENT 'Items whose status could not be mapped. Should be 0.',
    last_update                       TIMESTAMP NOT NULL   COMMENT 'When this record was last computed'
"""


@dp.expect_or_fail(
    "Total work must equal sum of all states",
    """
        total_work = (
            unstarted + in_progress + blocked + pending_review + merged_internally +
            merged_to_client + user_testing + done + removed + uncategorised
        )
    """,
)
@dp.materialized_view(
    name="releases",
    comment="Unified release table combining workload and progress metrics",
    schema=RELEASES_SCHEMA,
)
def releases():
    """
    Gold materialized view — aggregates delivery metrics per release.
    Powers the Business Analyst dashboard directly.

    Key decisions:
    - arrays_zip before explode: keeps release_id and release_name paired
      so exploding parallel arrays doesn't create an incorrect cross product.
    - effective_story_points: ADO preferred over JIRA via COALESCE because
      ADO reflects what the internal team actually estimates.
    - unified_state: maps both systems to a common taxonomy (see utils.py).
    - @dp.expect_or_fail: validates total_work == sum of all states.
      Pipeline fails before writing if the math doesn't add up.
      Better a loud failure than silent incorrect dashboard metrics.
    """
    df = spark.read.table(MAPPED_USER_STORIES_TABLE)

    exploded = df.withColumn(
        "release",
        F.explode(
            F.arrays_zip(
                F.col("release_ids"),
                F.col("jira_fix_versions"),
            )
        ),
    ).withColumn(
        "release_id", F.col("release.release_ids")
    ).withColumn(
        "release_name", F.col("release.jira_fix_versions")
    )

    exploded = exploded.withColumn(
        "effective_story_points",
        F.coalesce(
            F.col("ado_story_points").cast("double"),
            F.col("jira_story_points").cast("double"),
        ),
    )

    exploded = unified_state(exploded)

    result = (
        exploded.groupBy("release_id", "release_name")
        .agg(
            F.sum("effective_story_points").alias("story_point_total"),
            F.count("jira_id").alias("amount_of_work_items"),
            F.count_if(F.col("effective_story_points").isNull()).alias("work_items_without_story_points"),
            F.count("jira_id").alias("total_work"),
            F.count_if(F.col("unified_state") == "unstarted").alias("unstarted"),
            F.count_if(F.col("unified_state") == "in_progress").alias("in_progress"),
            F.count_if(F.col("unified_state") == "blocked").alias("blocked"),
            F.count_if(F.col("unified_state") == "pending_review").alias("pending_review"),
            F.count_if(F.col("unified_state") == "merged_internally").alias("merged_internally"),
            F.count_if(F.col("unified_state") == "merged_to_client").alias("merged_to_client"),
            F.count_if(F.col("unified_state") == "user_testing").alias("user_testing"),
            F.count_if(F.col("unified_state") == "done").alias("done"),
            F.count_if(F.col("unified_state") == "removed").alias("removed"),
            F.count_if(F.col("unified_state") == "uncategorised").alias("uncategorised"),
            F.current_timestamp().alias("last_update"),
        )
        .orderBy("release_name")
    )

    return result