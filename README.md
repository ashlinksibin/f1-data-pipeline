# F1 Racing Data Engineering Pipeline on Azure

An end-to-end data engineering project that ingests Formula 1 data from the Ergast API into Azure Data Lake Storage Gen2 using Azure Data Factory, with Azure Databricks planned for transformation and Power BI for reporting.

## Architecture

![Architecture](images/architecture.jpeg)

**Flow:** Ergast API → ADF Pipelines → ADLS Gen2 (Raw) → Databricks (Transform) → ADLS Gen2 (Presentation) → Analyze → Power BI

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Transformation | Azure Databricks (PySpark, Delta) |
| Governance and access | Unity Catalog, Access Connector, Azure RBAC |
| Reporting | Power BI |
| Source | Ergast F1 API (community mirror: api.jolpi.ca) |

## Azure Resources

- **Resource group:** `f1-dataeng-rg`
- **Storage account:** `f1dataengstorage01` with containers for the raw, ingested and presentation layers
- **Data Factory:** `adf-f1dataeng001`
- **Databricks workspace:** `db-f1dataeng001`

## What I Built

### 1. Ingest (completed)
- Created ADF linked services for the HTTP source (Ergast API) and ADLS Gen2 sink.
- Built source and sink datasets for five entities: **circuits, constructors, drivers, races, results**.
- Built an ingestion pipeline with Copy Data activities that land JSON files in the raw layer, one folder per entity (`raw-data/<entity>/<entity>.json`).

### 2. Secure access (completed)
- Assigned storage RBAC roles to the Databricks Access Connector.
- Created a Unity Catalog storage credential and external location pointing at the raw container, and validated read, write, list and delete permissions.

### 3. Transform (in progress)
- Read raw nested JSON in Databricks via `abfss://` paths.
- Planned: flatten the Ergast `MRData` structure with PySpark and write clean Delta tables to the ingested layer.

### 4. Analyze and Report (planned)
- Aggregate tables such as driver standings, constructor performance and race summaries.
- Power BI dashboard on top of the presentation layer.

## Repository Structure

```
.
├── README.md
├── images/              # architecture diagram and portal screenshots
├── adf/
│   ├── pipeline/        # exported pipeline JSON
│   ├── dataset/         # exported dataset JSON
│   └── linkedService/   # exported linked service JSON (no secrets)
└── databricks/
    └── notebooks/       # exported notebooks (.py)
```

## How to Reproduce

1. Create a resource group, storage account (hierarchical namespace enabled), Data Factory and Databricks workspace.
2. Create containers for the raw, ingested and presentation layers.
3. In ADF, create linked services for HTTP (`https://api.jolpi.ca/ergast/f1/`) and ADLS Gen2, then the source and sink datasets and a Copy Data pipeline.
4. Grant the Databricks Access Connector the **Storage Blob Data Contributor** (or Owner) role on the storage account.
5. In Databricks, create a storage credential and external locations, then read the raw JSON from a notebook.

## Challenges and Lessons Learned

- **Ergast API retirement:** the original endpoint is being phased out, so the pipeline targets the community-maintained mirror.
- **Schema import errors on sink datasets:** these appear before the first run because the file does not exist yet; running the pipeline creates it.
- **Unity Catalog vs account keys:** a leftover `fs.azure.account.key` Spark setting conflicts with Unity Catalog authentication, so use one auth method per notebook.
- **Cost control:** Azure free credits ran out, which taught me to use auto-terminating, small clusters and to export pipeline and notebook code early so work is not lost.

## Future Improvements

- Parameterize datasets and use a ForEach loop instead of one Copy activity per entity.
- Incremental loads by season and round.
- Store secrets in Azure Key Vault.
- Finish the Databricks transform and Power BI report stages.

## Author

**ASHLIN K SIBIN** · [LinkedIn](https://linkedin.com/in/ashlinksibin) · [GitHub](https://github.com/ashlinksibin)
