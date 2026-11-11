# SeaweedFS (object store)

SeaweedFS runs master, volume server, filer and S3 gateway in one container (`weed server`).
Every table format in the playground keeps its data files here, via the S3 API.

| Port | What                                   |
|------|----------------------------------------|
| 8333 | S3 gateway (`http://localhost:8333`)   |
| 8888 | Filer UI — browse buckets as folders   |
| 9333 | Master UI — volumes and cluster status |

Credentials come from `S3_ACCESS_KEY` / `S3_SECRET_KEY` in `.env`; `entrypoint.sh` renders them
into SeaweedFS's S3 identity config at start-up. Clients must use **path-style** addressing
(`http://seaweedfs:8333/<bucket>/<key>`) and region `us-east-1`.

Buckets (created by `make bootstrap`): `raw`, `ducklake`, `iceberg`, `delta`.

## Things to try

- Open http://localhost:8888/buckets/ and look at how each table format lays out files:
  DuckLake's plain Parquet, Iceberg's `metadata/` + `data/`, Delta's `_delta_log/`.
- `aws --endpoint-url http://localhost:8333 s3 ls s3://raw --recursive` (with the keys exported).
