"""
Bronze Azure DevOps User Stories ingestion pipeline.

Title
-----
azure_devops_ingestion_bronze.py

Description
-----------
Loads Azure DevOps User Stories into the bronze layer using a custom Spark
DataSource. Stores the raw payload for downstream processing.

"""

# COMMAND ----------
# DBTITLE 1, Imports
# Import registers the custom datasource


from pyspark import pipelines as dp
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StringType, StructField, StructType
from typing import Any, Dict, Iterator, List, Tuple
from utils.secrets import retrieve_secrets


class AzureDevOpsWorkItems(DataSource):
    """Custom datasource for Azure DevOps User Stories."""

    @classmethod
    def name(cls) -> str:
        return "azure_devops_workitems"

    def schema(self) -> StructType:
        return StructType(
            [
                StructField("org", StringType(), True),
                StructField("project", StringType(), True),
                StructField("id", StringType(), True),
                StructField("rev", StringType(), True),
                StructField("title", StringType(), True),
                StructField("url", StringType(), True),
                StructField("raw_data", StringType(), True),
            ]
        )

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        return AzureDevOpsWorkItemsReader(self.options)


class AzureDevOpsWorkItemsReader(SimpleDataSourceStreamReader):
    """Reader for Azure DevOps User Stories."""

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def _workspace_client(self):
        """Create the Databricks workspace client."""
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient(
            host=self.options["host"],
            client_id=self.options["client_id"],
            client_secret=self.options["client_secret"],
        )

    def _parse_projects(self) -> List[str] | None:
        """Parse the projects option."""
        raw = (self.options.get("projects") or "ALL").strip()

        if not raw or raw.upper() in ("ALL", "*"):
            return None

        return [project.strip() for project in raw.split(",") if project.strip()]

    def _list_projects(self, org: str) -> List[str]:
        """List all projects in the org."""
        from databricks.sdk.service.serving import ExternalFunctionRequestHttpMethod
        from urllib.parse import quote

        workspace_client = self._workspace_client()

        response = workspace_client.serving_endpoints.http_request(
            conn=self.options["connection"],
            method=ExternalFunctionRequestHttpMethod.GET,
            path=f"/{quote(org)}/_apis/projects?api-version=7.1",
            headers={"Accept": "application/json"},
        )

        data = response.json()
        return [
            project["name"]
            for project in data.get("value", [])
            if project.get("name")
        ]

    def initialOffset(self) -> dict:
        """Return the initial offset."""
        return {
            "last_update_timestamp": "2024-01-01T12:00:00Z",
            "last_work_item_id": 0,
        }

    def _read_batch_work_items(
        self,
        org: str,
        project: str,
        ids: List[int],
    ) -> List[Tuple[str, str, str, str, str | None, str, str]]:
        """Read work items in batches."""
        from databricks.sdk.service.serving import ExternalFunctionRequestHttpMethod
        from urllib.parse import quote
        import json

        if not ids:
            return []

        workspace_client = self._workspace_client()

        payload: Dict[str, Any] = {
            "$expand": "All",
            "ids": ids,
        }

        response = workspace_client.serving_endpoints.http_request(
            conn=self.options["connection"],
            method=ExternalFunctionRequestHttpMethod.POST,
            path=f"/{quote(org)}/{quote(project)}/_apis/wit/workitemsbatch?api-version=7.1",
            json=payload,
            headers={"Accept": "application/json"},
        )

        return [
            (
                org,
                project,
                str(item["id"]),
                str(item["rev"]),
                item.get("fields", {}).get("System.Title"),
                item["url"],
                json.dumps(item),
            )
            for item in response.json().get("value", [])
        ]

    def read(self, start: dict) -> Tuple[Iterator[Tuple], dict]:
        """Read new or changed User Stories."""
        from databricks.sdk.service.serving import ExternalFunctionRequestHttpMethod
        from datetime import datetime, timezone
        from urllib.parse import quote

        workspace_client = self._workspace_client()
        org = self.options["org"]
        last_update_timestamp = start["last_update_timestamp"]
        last_work_item_id = start["last_work_item_id"]
        current_timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        configured_projects = self._parse_projects()
        projects = (
            configured_projects
            if configured_projects is not None
            else self._list_projects(org)
        )

        all_work_items: List[Tuple] = []
        max_work_item_id = last_work_item_id

        for project in projects:
            wiql = (
                "SELECT [System.Id] "
                "FROM workitems "
                "WHERE [System.WorkItemType] = 'User Story' "
                f"AND [System.ChangedDate] >= '{last_update_timestamp}' "
                f"AND [System.Id] > {last_work_item_id} "
                "ORDER BY [System.Id] ASC"
            )

            payload: Dict[str, Any] = {"query": wiql}

            response = workspace_client.serving_endpoints.http_request(
                conn=self.options["connection"],
                method=ExternalFunctionRequestHttpMethod.POST,
                path=f"/{quote(org)}/{quote(project)}/_apis/wit/wiql?api-version=7.1&timePrecision=true",
                json=payload,
                headers={"Accept": "application/json"},
            )

            data = response.json()
            ids = [item["id"] for item in data.get("workItems", [])]

            if ids:
                max_work_item_id = max(max_work_item_id, max(ids))

            for index in range(0, len(ids), 200):
                all_work_items.extend(
                    self._read_batch_work_items(
                        org,
                        project,
                        ids[index:index + 200],
                    )
                )

        end_offset = {
            "last_update_timestamp": current_timestamp_utc,
            "last_work_item_id": max_work_item_id,
        }

        return iter(all_work_items), end_offset

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
        """Read data between offsets."""
        return self.read(start)[0]

    def commit(self, end: dict) -> None:
        """Commit the offset."""
        pass


secrets = retrieve_secrets([AZURE_DEVOPS_CLIENT_ID_KEY, AZURE_DEVOPS_CLIENT_SECRET_KEY])

AZURE_DEVOPS_CLIENT_ID = secrets[AZURE_DEVOPS_CLIENT_ID_KEY]
AZURE_DEVOPS_CLIENT_SECRET = secrets[AZURE_DEVOPS_CLIENT_SECRET_KEY]


# Register the datasource
spark.dataSource.register(AzureDevOpsWorkItems)


# COMMAND ----------
# DBTITLE 1, Bronze Table Ingestion
BRONZE_SCHEMA = spark.conf.get("DEFAULT_BRONZE_ADO_SCHEMA")


@dp.table(
    name=f"{BRONZE_SCHEMA}.azure_devops_user_stories",
    comment="Bronze Azure DevOps User Stories ingestion table.",
)
def bronze_azure_devops_user_stories():
    """Create the bronze User Stories table."""
    bronze = (
        spark.readStream
        .format("azure_devops_workitems")
        .options(
            host=AZURE_DEVOPS_HOST,
            client_id=AZURE_DEVOPS_CLIENT_ID,
            client_secret=AZURE_DEVOPS_CLIENT_SECRET,
            org=AZURE_DEVOPS_ORG,
            connection=AZURE_DEVOPS_CONNECTION,
            projects=AZURE_DEVOPS_PROJECTS,
        )
        .load()
        .withColumn("extraction_timestamp", current_timestamp())
    )

    return bronze