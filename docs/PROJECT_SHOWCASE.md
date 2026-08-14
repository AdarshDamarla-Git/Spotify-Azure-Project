# Project showcase

## One-line summary

Built a metadata-driven Azure lakehouse that incrementally ingests Spotify-style SQL data with ADF, processes new Parquet files with Databricks Auto Loader, and publishes SCD-managed Delta facts and dimensions.

## Portfolio description

This project implements an end-to-end Spotify analytics pipeline on Azure. Azure Data Factory reads metadata from ADLS, loops across five Azure SQL tables, extracts only records newer than each table's persisted watermark, and writes compressed Parquet to the Bronze zone. ADF then triggers Databricks, where Auto Loader incrementally processes new files, PySpark cleans and enriches the data, and Lakeflow CDC flows publish SCD Type 2 dimensions and a Type 1 stream fact. The design includes Unity Catalog organization, data-quality expectations, Gold exports, and Databricks Asset Bundle targets.

## Resume bullets

- Engineered a metadata-driven ADF pipeline that incrementally ingests five Azure SQL tables into ADLS using table-specific CDC columns and persisted watermarks.
- Implemented dynamic datasets, sequential table loops, empty-file cleanup, conditional watermark updates, and downstream Databricks job orchestration.
- Built PySpark Auto Loader transformations with schema evolution, checkpointing, deduplication, standardization, and Delta Lake persistence.
- Designed Lakeflow Gold pipelines with SCD Type 2 user, artist, track, and date dimensions plus an SCD Type 1 streaming fact.

## Interview talking points

### Why store watermarks in ADLS?

The control state is durable, inexpensive, and accessible to ADF. A per-table file lets each source advance independently.

### Why use a metadata-driven loop?

The same ingestion logic handles all tables. Adding another table primarily requires a metadata entry rather than a cloned pipeline branch.

### Why Auto Loader after ADF?

ADF handles source extraction and orchestration, while Auto Loader efficiently discovers new lake files, persists schema state, and tracks processed inputs through checkpoints.

### Why SCD Type 2?

Subscription, artist, and track attributes can change. Type 2 retains earlier values and their effective periods for historical analytics.

## Suggested GitHub topics

```text
azure-data-factory
azure-sql
adls-gen2
azure-databricks
pyspark
databricks-autoloader
delta-lake
lakeflow
unity-catalog
incremental-load
change-data-capture
scd-type-2
spotify-data
data-engineering
```

## Suggested repository description

Metadata-driven Spotify lakehouse using Azure SQL, ADF incremental watermarks, ADLS Gen2, Databricks Auto Loader, PySpark, Delta Lake, and SCD2 modeling.
