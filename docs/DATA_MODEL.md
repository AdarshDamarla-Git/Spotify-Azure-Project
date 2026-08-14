# Data model

## Spotify analytical star schema

```mermaid
erDiagram
    DIM_USER ||--o{ FACT_STREAM : user_id
    DIM_TRACK ||--o{ FACT_STREAM : track_id
    DIM_DATE ||--o{ FACT_STREAM : date_key
    DIM_ARTIST ||--o{ DIM_TRACK : artist_id

    FACT_STREAM {
        bigint stream_id
        int user_id
        int track_id
        int date_key
        int listen_duration
        string device_type
        timestamp stream_timestamp
    }
    DIM_USER {
        int user_id
        string user_name
        string country
        string subscription_type
        date start_date
        date end_date
        timestamp updated_at
        timestamp __START_AT
        timestamp __END_AT
    }
    DIM_ARTIST {
        int artist_id
        string artist_name
        string genre
        string country
        timestamp updated_at
        timestamp __START_AT
        timestamp __END_AT
    }
    DIM_TRACK {
        int track_id
        string track_name
        int artist_id
        string album_name
        int duration_sec
        string durationFlag
        date release_date
        timestamp updated_at
        timestamp __START_AT
        timestamp __END_AT
    }
    DIM_DATE {
        int date_key
        date date
        int day
        int month
        int year
        string weekday
        timestamp __START_AT
        timestamp __END_AT
    }
```

## Gold table grains

| Table | Grain | Gold history |
|---|---|---|
| `dimuser` | One row per user version | SCD Type 2 by `updated_at` |
| `dimartist` | One row per artist version | SCD Type 2 by `updated_at` |
| `dimtrack` | One row per track version | SCD Type 2 by `updated_at` |
| `dimdate` | One row per date version | SCD Type 2 by `date` |
| `factstream` | One current row per stream event | SCD Type 1 by `stream_timestamp` |

## Track duration classification

| Rule | `durationFlag` |
|---|---|
| `duration_sec < 150` | `low` |
| `150 <= duration_sec < 300` | `medium` |
| `duration_sec >= 300` | `high` |

## Example analytics

```sql
SELECT
  a.artist_name,
  COUNT(*) AS streams,
  SUM(f.listen_duration) AS listening_seconds
FROM spotify_catalog.gold.factstream AS f
JOIN spotify_catalog.gold.dimtrack AS t
  ON f.track_id = t.track_id AND t.__END_AT IS NULL
JOIN spotify_catalog.gold.dimartist AS a
  ON t.artist_id = a.artist_id AND a.__END_AT IS NULL
GROUP BY a.artist_name
ORDER BY listening_seconds DESC
LIMIT 20;
```

## Modeling considerations

- Historical reporting should join facts to the dimension version effective at stream time, not always the current version.
- `DimDate` usually behaves as a static dimension; Type 2 may be unnecessary unless calendar attributes can change.
- Add relationship tests for fact user, track, and date keys.
- Define whether `listen_duration` is seconds and document valid ranges.
- Consider a dedicated album dimension if album-level analysis grows.
