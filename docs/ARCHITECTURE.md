# Architecture

## System context

```mermaid
flowchart LR
    SQL["Azure SQL Database<br/>Spotify operational data"]
    ADF["Azure Data Factory<br/>incremental orchestration"]
    ADLS["ADLS Gen2<br/>medallion storage"]
    DBX["Azure Databricks<br/>Spark + Lakeflow"]
    BI["Analytics consumers"]

    SQL --> ADF --> ADLS --> DBX --> BI
    DBX --> ADLS
```

## Detailed component architecture

```mermaid
flowchart TB
    subgraph SOURCE["Source system"]
        U["DimUser"]
        A["DimArtist"]
        T["DimTrack"]
        D["DimDate"]
        F["FactStream"]
    end

    subgraph FACTORY["Azure Data Factory"]
        TRIGGER["Schedule / manual trigger"]
        LOOKUP["Lookup loop_input.json"]
        FOREACH["Sequential ForEach"]
        LAST["Lookup prior CDC watermark"]
        CURRENT["Capture run timestamp"]
        COPY["Dynamic Azure SQL → Parquet Copy"]
        CONDITION["Data read > 0?"]
        DELETE["Delete empty file"]
        MAX["Query maximum CDC"]
        UPDATE["Write updated cdc.json"]
        DBJOB["DatabricksJob activity"]

        TRIGGER --> LOOKUP --> FOREACH
        FOREACH --> LAST & CURRENT
        LAST --> COPY
        CURRENT --> COPY
        COPY --> CONDITION
        CONDITION -->|"No"| DELETE
        CONDITION -->|"Yes"| MAX --> UPDATE
        FOREACH --> DBJOB
    end

    subgraph LAKE["ADLS Gen2"]
        META["bronze/Database_tables_input"]
        CDC["bronze/<Table>_cdc/cdc.json"]
        BRONZE["bronze/<Table>/*.parquet"]
        SILVERDATA["silver/<Table>/data"]
        SCHEMA["silver/<Table>/schema"]
        CHECKPOINT["silver/<Table>/checkpoint"]
        GOLDEXPORT["gold/<Table>/*.parquet"]
    end

    subgraph DATABRICKS["Azure Databricks"]
        AUTO["Auto Loader"]
        TRANSFORM["PySpark transformations"]
        ST["Silver Delta tables"]
        FLOW["Lakeflow AUTO CDC"]
        GT["Gold facts + dimensions"]
        EXPORT["Gold export notebook"]

        AUTO --> TRANSFORM --> ST --> FLOW --> GT --> EXPORT
    end

    META --> LOOKUP
    CDC --> LAST
    SOURCE --> COPY --> BRONZE
    UPDATE --> CDC
    DBJOB --> AUTO
    BRONZE --> AUTO
    AUTO --> SCHEMA & CHECKPOINT
    TRANSFORM --> SILVERDATA
    EXPORT --> GOLDEXPORT
```

## Orchestration sequence

```mermaid
sequenceDiagram
    autonumber
    participant T as ADF Trigger
    participant M as ADLS Metadata
    participant S as Azure SQL
    participant B as ADLS Bronze
    participant D as Databricks Job

    T->>M: Read loop_input.json
    loop Each configured table, sequentially
        T->>M: Read prior table watermark
        T->>T: Capture current run time
        T->>S: Query rows newer than watermark
        S-->>T: Incremental result
        T->>B: Write timestamped Parquet
        alt Data was extracted
            T->>S: Query max CDC value
            S-->>T: New watermark
            T->>M: Overwrite table cdc.json
        else No data
            T->>B: Delete empty output
        end
    end
    T->>D: Run Databricks processing job
    D-->>T: Completion state
```

## Databricks processing lineage

```mermaid
flowchart LR
    B["Bronze Parquet"] --> AL["Auto Loader"] --> S["Silver Delta"]
    S --> SU["dimuser_stg"] --> GU["Gold dimuser<br/>SCD2"]
    S --> SA["dimartist_stg"] --> GA["Gold dimartist<br/>SCD2"]
    S --> ST["dimtrack_stg"] --> GT["Gold dimtrack<br/>SCD2"]
    S --> SD["dimdate_stg"] --> GD["Gold dimdate<br/>SCD2"]
    S --> SF["factstream_stg"] --> GF["Gold factstream<br/>SCD1"]
```

## Responsibility boundaries

| Component | Responsibility |
|---|---|
| Azure SQL | Source-of-record tables and CDC columns |
| ADF metadata | Declares which tables and columns participate |
| ADF pipeline | Extracts deltas, advances watermarks, and triggers processing |
| ADLS Bronze | Stores source-aligned incremental files and control state |
| Auto Loader | Discovers new files and tracks schema/checkpoint state |
| Silver PySpark | Cleans names, derives duration bands, deduplicates, and writes Delta |
| Gold Lakeflow | Enforces required IDs and applies SCD behavior |
| Unity Catalog | Provides governed table namespaces |
| Asset Bundle | Defines environment deployment targets |

## Failure boundaries

- A missing control file prevents the metadata lookup or watermark lookup from succeeding.
- Failure in one sequential table iteration stops later tables and prevents the Databricks job.
- A successful extract followed by failed watermark update may cause the same rows to be extracted again.
- Advancing a watermark too far can skip rows; see [Incremental ingestion](INCREMENTAL_INGESTION.md).
- Auto Loader checkpoint loss can cause replay behavior.
- Invalid Gold business keys are dropped by expectations.
