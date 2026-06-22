"""
Shared utilities for the Client Dashboard pipeline.
"""

from pyspark.sql import functions as F
from solution_code import ADO_STATUS_MAPPING, JIRA_STATUS_MAPPING


def _normalise_status(col_name: str):
    """
    Normalises a status column to improve mapping consistency.

    Transformations applied:
    - Trims leading/trailing whitespace
    - Converts to lowercase
    - Collapses multiple spaces into a single space

    Parameters
    ----------
    col_name : str
        Name of the column to normalise.

    Returns
    -------
    Column
        A Spark Column expression with normalised text.
    """
    normalised = F.regexp_replace(
        F.trim(F.lower(F.col(col_name))),
        r"\s+",
        " "
    )
    return normalised


def unified_state(df, ado_status_col: str = "ado_board_column", jira_status_col: str = "jira_status"):
    """
    Maps ADO and JIRA statuses to a single unified delivery state using DataFrame.replace().
    ADO board column is the primary source; JIRA status is the fallback when ADO is NULL.

    Both columns are normalised before mapping (lowercase, trimmed, collapsed spaces).
    If neither ADO nor JIRA status can be mapped, the unified state is set to 'uncategorised'.

    The mappings are defined in ADO_STATUS_MAPPING and JIRA_STATUS_MAPPING
    in solution_code/__init__.py. To add or update a status, modify those
    dictionaries — no changes needed here.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing the status columns to map.
    ado_status_col : str
        Name of the ADO board column. Defaults to 'ado_board_column'.
    jira_status_col : str
        Name of the JIRA status column. Defaults to 'jira_status'.

    Returns
    -------
    DataFrame
        The input DataFrame with an additional 'unified_state' column.
        Returns 'uncategorised' for any status not found in either mapping.
    """
    df = df.withColumn("_ado_normalised", _normalise_status(ado_status_col))
    df = df.withColumn("_jira_normalised", _normalise_status(jira_status_col))

    df = df.na.replace(ADO_STATUS_MAPPING, subset=["_ado_normalised"])
    df = df.na.replace(JIRA_STATUS_MAPPING, subset=["_jira_normalised"])

    df = df.withColumn(
        "unified_state",
        F.coalesce(
            F.when(F.col("_ado_normalised").isin(list(ADO_STATUS_MAPPING.values())), F.col("_ado_normalised")),
            F.when(F.col("_jira_normalised").isin(list(JIRA_STATUS_MAPPING.values())), F.col("_jira_normalised")),
            F.lit("uncategorised")
        )
    )

    df = df.drop("_ado_normalised", "_jira_normalised")

    return df