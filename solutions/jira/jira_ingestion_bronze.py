"""
Bronze JIRA Issues ingestion pipeline.

Description
-----------
Loads JIRA Issues into the bronze layer using a custom Spark
DataSource. Stores the raw payload for downstream processing.
"""

import solution_code  # noqa: F401

from pyspark import pipelines as dp
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StringType, StructField, StructType
from typing import Any, Dict, Iterator, List, Tuple
from utils.secrets import retrieve_secrets


class JiraIssues(DataSource):
    """
    Custom Spark DataSource for JIRA Issues.
    Defines the contract — name and schema.
    The actual API logic lives in JiraIssuesReader.
    """

    @classmethod
    def name(cls) -> str:
        """Name used in spark.readStream.format('jira_issues')."""
        return "jira_issues"

    def schema(self) -> StructType:
        """
        Schema Spark expects from this source.
        raw_data stores the full JSON payload — Bronze preserves everything,
        parsing happens downstream in Silver.
        """
        return StructType(
            [
                StructField("jira_internal_id", StringType(), True),
                StructField("project", StringType(), True),
                StructField("key", StringType(), True),
                StructField("summary", StringType(), True),
                StructField("issue_type", StringType(), True),
                StructField("updated", StringType(), True),
                StructField("url", StringType(), True),
                StructField("raw_data", StringType(), True),
            ]
        )

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        """
        Factory method required by the DataSource interface.
        Called by Spark when reading in streaming mode.
        Returns JiraIssuesReader with the options (credentials, domain).
        """
        return JiraIssuesReader(self.options)


class JiraIssuesReader(SimpleDataSourceStreamReader):
    """
    Implements the actual API calls to JIRA.
    Handles pagination, timezone conversion, and offset management.
    Imports are inside methods intentionally — custom DataSources are
    serialized to Spark executors where top-level imports can fail.
    """
    CHECKPOINT_FORMAT = "%Y-%m-%d %H:%M"

    def __init__(self, options: Dict[str, str]):
        self.options = options
        self._cloud_id = None
        self._jira_user_timezone = None

    def _get_auth(self):
        """HTTP Basic Auth for the Atlassian API using email and api_token."""
        from requests.auth import HTTPBasicAuth
        return HTTPBasicAuth(
            self.options["email"],
            self.options["api_token"],
        )

    def _get_cloud_id(self) -> str:
        """
        Fetches and caches the Atlassian cloud ID.
        Required for all API calls. Cached to avoid repeated requests per batch.
        """
        import requests

        if self._cloud_id is not None:
            return self._cloud_id

        domain = self.options["jira_domain"]
        response = requests.get(
            f"https://{domain}/_edge/tenant_info",
            headers={"Accept": "application/json"},
            auth=self._get_auth(),
            timeout=60,
        )
        response.raise_for_status()
        cloud_id = response.json()["cloudId"]
        self._cloud_id = cloud_id
        return cloud_id

    def _get_jira_user_timezone(self) -> str:
        """
        Fetches and caches the authenticated user's timezone.
        Critical — Jira's JQL filters by user local time, not UTC.
        Without this, the incremental filter silently misses records.
        """
        import requests

        if self._jira_user_timezone is not None:
            return self._jira_user_timezone

        cloud_id = self._get_cloud_id()
        response = requests.get(
            f"https://api.atlassian.com/ex/jira/{cloud_id}/rest/api/3/myself",
            headers={"Accept": "application/json"},
            auth=self._get_auth(),
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        jira_user_timezone = data.get("timeZone", "UTC")
        self._jira_user_timezone = jira_user_timezone
        return jira_user_timezone

    def _parse_project(self) -> str | None:
        """Parse the projects option. Returns None for ALL."""
        raw = (self.options.get("project") or "").strip()
        if not raw:
            return None
        return raw

    def _utc_checkpoint_to_jira_local(self, utc_checkpoint: str) -> str:
        """
        Converts UTC checkpoint to Jira user local time.
        Jira's JQL updated >= filter uses the authenticated user's timezone.
        Sending UTC directly would cause records to be silently missed.
        """
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo

        jira_timezone = self._get_jira_user_timezone()
        utc_dt = datetime.strptime(utc_checkpoint, self.CHECKPOINT_FORMAT).replace(
            tzinfo=timezone.utc
        )
        jira_local_dt = utc_dt.astimezone(ZoneInfo(jira_timezone))
        return jira_local_dt.strftime(self.CHECKPOINT_FORMAT)

    def _current_utc_checkpoint(self) -> str:
        """Current UTC timestamp truncated to minute — used as the end offset."""
        from datetime import datetime, timezone
        current_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        return current_utc.strftime(self.CHECKPOINT_FORMAT)

    def _build_jql(self, project: str | None, last_update_utc: str) -> str:
        """
        Builds the JQL query for incremental reads.
        Converts the UTC checkpoint to Jira local time before filtering.
        """
        clauses = []
        if project is not None:
            clauses.append(f'project = "{project}"')
        last_update_jira_local = self._utc_checkpoint_to_jira_local(last_update_utc)
        clauses.append(f'updated >= "{last_update_jira_local}"')
        return " AND ".join(clauses) + " ORDER BY updated ASC"

    def initialOffset(self) -> dict:
        """
        Starting point for the very first pipeline run.
        Set to year 2000 to load full history on initial execution.
        """
        return {"last_update_timestamp": "2000-01-01 00:00"}

    def read(self, start: dict) -> Tuple[Iterator[Tuple], dict]:
        """
        Core method called by DLT on each pipeline run.
        Fetches only issues updated since start['last_update_timestamp'].
        Paginates through ALL pages using nextPageToken before returning.
        Returns (records, end_offset) — DLT persists end_offset as checkpoint.
        If pipeline fails, DLT retries from the same start offset — no data loss.
        """
        import json
        import requests

        cloud_id = self._get_cloud_id()
        domain = self.options["jira_domain"]
        last_update_utc = start["last_update_timestamp"]
        current_utc_checkpoint = self._current_utc_checkpoint()

        project = self._parse_project()
        jql = self._build_jql(project, last_update_utc)

        url = f"https://api.atlassian.com/ex/jira/{cloud_id}/rest/api/3/search/jql"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        all_issues: List[Tuple] = []
        next_page_token = None
        total_issues = 0

        while True:
            payload: Dict[str, Any] = {
                "fields": ["*all"],
                "fieldsByKeys": True,
                "jql": jql,
                "maxResults": 100,
                "nextPageToken": next_page_token,
            }

            response = requests.post(
                url, json=payload, headers=headers,
                auth=self._get_auth(), timeout=120,
            )
            response.raise_for_status()
            data = response.json()
            page_issues = data.get("issues", [])

            for issue in page_issues:
                fields = issue.get("fields", {})
                project_key = fields.get("project", {}).get("key", "")
                all_issues.append((
                    issue.get("id", ""),
                    project_key,
                    issue.get("key", ""),
                    fields.get("summary", ""),
                    fields.get("issuetype", {}).get("name", ""),
                    fields.get("updated", ""),
                    f"https://{domain}/browse/{issue.get('key', '')}",
                    json.dumps(issue),
                ))

            total_issues += len(page_issues)
            next_page_token = data.get("nextPageToken")

            if not next_page_token:
                break

        end_offset = {"last_update_timestamp": current_utc_checkpoint}
        return iter(all_issues), end_offset

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
        """Required by the interface for bounded reads between two offsets."""
        return self.read(start)[0]

    def commit(self, end: dict) -> None:
        """Offset commit — DLT handles checkpoint persistence automatically."""
        pass


spark.dataSource.register(JiraIssues)

BRONZE_SCHEMA = spark.conf.get("DEFAULT_BRONZE_CLIENT_SCHEMA")


@dp.table(
    name=f"{BRONZE_SCHEMA}.jira_issues",
    comment="Bronze JIRA Issues ingestion table.",
)
def bronze_jira_issues():
    """
    DLT Bronze table — raw JIRA issues ingested via the custom DataSource.
    Reads credentials from Databricks Secrets (never hardcoded).
    Adds extraction_timestamp for audit trail.
    No transformation — Bronze stores raw data exactly as received.
    """
    JIRA_API_USERNAME_SECRET_NAME = spark.conf.get("JIRA_USERNAME_SECRET_NAME")
    JIRA_API_TOKEN_SECRET_NAME = spark.conf.get("JIRA_API_TOKEN_SECRET_NAME")
    JIRA_DOMAIN = spark.conf.get("JIRA_DOMAIN")
    JIRA_PROJECT_NAME = spark.conf.get("JIRA_PROJECT_NAME")

    secrets = retrieve_secrets([JIRA_API_USERNAME_SECRET_NAME, JIRA_API_TOKEN_SECRET_NAME])
    JIRA_API_USERNAME = secrets[JIRA_API_USERNAME_SECRET_NAME]
    JIRA_API_TOKEN = secrets[JIRA_API_TOKEN_SECRET_NAME]

    return (
        spark.readStream.format("jira_issues")
        .options(
            jira_domain=JIRA_DOMAIN,
            email=JIRA_API_USERNAME,
            api_token=JIRA_API_TOKEN,
            project=JIRA_PROJECT_NAME,
        )
        .load()
        .withColumn("extraction_timestamp", current_timestamp())
    )