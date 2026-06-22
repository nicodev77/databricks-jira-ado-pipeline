# Databricks Medallion Pipeline — Jira & Azure DevOps Integration

A production data pipeline built on **Databricks Delta Live Tables** that integrates two external project management systems — a client-side Jira instance and an internal Azure DevOps organization — into a unified delivery dashboard.

## Problem

Two systems tracking the same work with no visibility between them:
- **Jira** — client-facing project tracker
- **Azure DevOps** — internal development tracker

The Business Analyst had no way to track delivery status across both platforms. This pipeline solves that.

## Architecture

```
Jira API                    Azure DevOps API
    ↓                              ↓
Bronze (raw JSON)           Bronze (raw JSON)
    ↓                              ↓
Silver (parsed + CDC)       Silver (parsed + CDC)
    ↓                              ↓
         Gold (mapped_user_stories)
                    ↓
            Gold (releases)
                    ↓
         Databricks Dashboard
```

## Tech Stack

- **Databricks Delta Live Tables (DLT)** — pipeline orchestration and CDC
- **Delta Lake** — ACID transactions, Time Travel, SCD Type 1
- **PySpark** — distributed data processing
- **Python** — custom DataSources, API integrations
- **Databricks Asset Bundles** — infrastructure as code (YML)

## Key Technical Highlights

### Custom PySpark DataSources
Both Jira and Azure DevOps are implemented as native Spark streaming sources via the `DataSource` and `SimpleDataSourceStreamReader` interfaces. This enables:
- Automatic checkpoint management by DLT
- Offset-based incremental reading
- Seamless integration with the DLT pipeline graph

### Incremental Ingestion
- **Jira**: cursor-based pagination via `nextPageToken` with timezone-aware JQL filtering
- **Azure DevOps**: WIQL-based querying via Databricks SDK with batch processing

### CDC in Silver Layer
Both Silver layers use `dp.create_auto_cdc_flow` with SCD Type 1 — each record is upserted based on its business key, always reflecting current state.

### Data Quality
- `@dp.expect` — non-blocking quality checks (monitors unmapped stories)
- `@dp.expect_or_fail` — blocking integrity checks (validates release math before writing to dashboard)

### Infrastructure as Code
All pipeline and job configurations are defined as **Databricks Asset Bundles** in YML — version-controlled, reproducible, and deployed via CI/CD.

## Project Structure

```
├── jira_ingestion_bronze.py          # Custom DataSource + Bronze DLT table for Jira
├── jira_transformations_silver.py    # Silver layer with CDC for Jira
├── azure_devops_ingestion_bronze.py  # Custom DataSource + Bronze DLT table for ADO
├── azure_devops_transformations_silver.py  # Silver layer with CDC for ADO
├── client_user_story_mapping.py      # Gold — maps Jira to ADO user stories
├── client_releases.py                # Gold — aggregates metrics per release
├── utils.py                          # Shared utilities — unified state mapping
├── jira_pipeline.yml                 # DLT pipeline definition (Asset Bundle)
├── jira_extraction_daily.yml         # Job schedule for Jira pipeline
├── azure_devops_pipeline.yml         # DLT pipeline definition (Asset Bundle)
└── azure_devops_extraction_daily.yml # Job schedule for ADO pipeline
```

## Author

Nicolas Chamorro Ferreira
- [LinkedIn](https://www.linkedin.com/in/nicolas-chamorro-ferreira)
- Databricks Certified Data Engineer Associate & Professional
