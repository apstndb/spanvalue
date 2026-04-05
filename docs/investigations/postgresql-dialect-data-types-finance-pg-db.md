# PostgreSQL-dialect Spanner: data types investigation notes

**Status:** reference only (not required for `spanvalue` behavior).  
**Environment (public sample):** project `gcpug-public-spanner`, instance `merpay-sponsored-instance`, database **`finance-pg-db`**.  
**Primary doc reviewed:** [PostgreSQL data types](https://cloud.google.com/spanner/docs/reference/postgresql/data-types) (fetched via `dkcli get` / Cloud Console mirror).

Investigation was performed to cross-check the supported-type table, array/vector/serial/interval rules, unsupported types, and Go client `ResultSetMetadata` behavior.

---

## Objects created on `finance-pg-db`

| Object | Purpose |
|--------|---------|
| `public.spanvalue_pg_dtype_probe` | One column per **storable** scalar/array type from the doc (plus aliases: `real`, `double precision`, `decimal`, `timestamp with time zone`, `boolean`, etc.). Includes `float8[] VECTOR LENGTH 4`. |
| `public.spanvalue_serial_probe` | **`serial`** primary key after `default_sequence_kind` was set (see below). |

---

## `ALTER DATABASE` (allowed on this database)

Serial types require a default sequence kind. Applied:

```sql
ALTER DATABASE "finance-pg-db" SET spanner.default_sequence_kind = 'bit_reversed_positive';
```

Verified via `information_schema.database_options`:

| option_name | option_value |
|-------------|--------------|
| `database_dialect` | `POSTGRESQL` |
| `default_sequence_kind` | `bit_reversed_positive` |

Then:

```sql
CREATE TABLE spanvalue_serial_probe (
  id serial PRIMARY KEY,
  note text
);
```

`information_schema.columns` (abridged):

| column_name | data_type | spanner_type | is_identity |
|-------------|-----------|--------------|-------------|
| `id` | `bigint` | `bigint` | `YES` |
| `note` | `character varying` | `character varying` | `NO` |

This matches the doc: serial aliases map to **identity `bigint`**, not a separate `serial` type in catalog metadata.

---

## `spanvalue_pg_dtype_probe` — `information_schema.columns` snapshot

Query:

```sql
SELECT column_name, data_type, spanner_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'spanvalue_pg_dtype_probe'
ORDER BY ordinal_position;
```

Observations:

- **`text`** columns appear as **`spanner_type` = `character varying`** (no length), consistent with Spanner’s catalog rendering—not necessarily identical to PostgreSQL’s `pg_catalog` labels.
- **`jsonb` / `numeric`** use PostgreSQL names in `data_type` and `spanner_type`.
- **Arrays** use `data_type = ARRAY` and `spanner_type` like `bigint[]`, `boolean[]`, `character varying(8)[]`.
- **Vector extension:** `float8[] VECTOR LENGTH 4` appears as `double precision[] vector length 4` in `spanner_type`.

---

## `interval` (query-only)

Doc: cannot be stored in a table column; expressions/views may use it.

```sql
SELECT CAST('P1Y2M3DT4H5M6.5S' AS INTERVAL) AS iv;
```

Returns a normalized ISO 8601 interval string (e.g. `P1Y2M3DT4H5M6.5S`).

---

## Unsupported / negative checks (doc “Unsupported” + limitations)

| Check | Outcome |
|-------|---------|
| `TIMESTAMP WITHOUT TIME ZONE` | Error: `The Postgres Type is not supported: timestamp without time zone` |
| `CHAR` / `bpchar` in DDL | Error: `Type <bpchar> is not supported.` |
| Multi-dimensional array DDL `int8[2][3]` | Error: `Multi-dimensional arrays are not supported.` |
| `serial` before `default_sequence_kind` | Error asking to set `default_sequence_kind` (or specify sequence kind on the identity column). |

---

## `pg_catalog` / OID

- Unqualified `pg_type` may fail; use **`pg_catalog.pg_type`**.
- Example: `SELECT oid, typname FROM pg_catalog.pg_type WHERE typname IN ('bool','int8','jsonb');`
- For **`oid` columns** in `information_schema.columns` (`pg_catalog` tables), `data_type` and `spanner_type` show as **`oid`**.
- Go client: `SELECT oid FROM pg_catalog.pg_type WHERE typname = 'bool' LIMIT 1` → metadata field type **`INT64`** with **`TypeAnnotationCode_PG_OID`**.

---

## Go client: metadata with **zero rows**

For `cloud.google.com/go/spanner.RowIterator`, `Metadata` is populated **after the first `Next()`** (see package doc on `RowIterator.Metadata`).  

Even when the first `Next()` returns `iterator.Done` (no rows), **`Metadata` / `RowType` are still populated** if the query succeeds. Example:

```sql
SELECT * FROM spanvalue_pg_dtype_probe WHERE FALSE;
```

Use this pattern to inspect column types (including `PG_NUMERIC` / `PG_JSONB` on stored columns) without reading data rows.

---

## DDL application method

DDL must use **`gcloud spanner databases ddl update`** (or Admin API). **`gcloud spanner databases execute-sql` cannot run DDL** (returns `DDL statements cannot be issued as SELECT/DML`).

---

## Cleanup (optional)

If you need to remove the probe tables:

```sql
DROP TABLE spanvalue_serial_probe;
DROP TABLE spanvalue_pg_dtype_probe;
```

To reset the database option (only if appropriate for your environment):

```sql
ALTER DATABASE "finance-pg-db" SET spanner.default_sequence_kind = DEFAULT;
```

---

## Repro commands (abbreviated)

```bash
export SPANNER_PROJECT_ID=gcpug-public-spanner
export SPANNER_INSTANCE_ID=merpay-sponsored-instance
export SPANNER_DATABASE_ID=finance-pg-db

gcloud spanner databases execute-sql "$SPANNER_DATABASE_ID" \
  --project="$SPANNER_PROJECT_ID" --instance="$SPANNER_INSTANCE_ID" \
  --sql='SELECT column_name, data_type, spanner_type FROM information_schema.columns
         WHERE table_schema = '\''public'\'' AND table_name = '\''spanvalue_pg_dtype_probe'\''
         ORDER BY ordinal_position'
```

---

*Last updated from interactive verification against `finance-pg-db`.*
