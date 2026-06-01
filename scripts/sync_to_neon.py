import os
from typing import Iterable, List, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


BATCH_SIZE = 1000
TABLES = ["realtime_traffic_weather", "realtime_camera"]


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def chunked(rows: Sequence[Tuple], size: int) -> Iterable[Sequence[Tuple]]:
    for idx in range(0, len(rows), size):
        yield rows[idx : idx + size]


def resolve_primary_keys(columns: List[str]) -> List[str]:
    if "time" in columns:
        return ["location_name", "time"]
    if "timestamp" in columns:
        return ["location_name", "timestamp"]
    raise RuntimeError("Could not find expected time column for primary key.")


def fetch_rows(conn, table: str) -> Tuple[List[str], List[Tuple]]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {quote_ident(table)}")
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return columns, rows


def build_upsert_sql(table: str, columns: List[str], pk_cols: List[str]) -> str:
    column_list = ", ".join(quote_ident(col) for col in columns)
    pk_list = ", ".join(quote_ident(col) for col in pk_cols)
    update_cols = [col for col in columns if col not in pk_cols]

    if update_cols:
        set_clause = ", ".join(
            f"{quote_ident(col)} = EXCLUDED.{quote_ident(col)}" for col in update_cols
        )
        conflict_clause = f"ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}"
    else:
        conflict_clause = f"ON CONFLICT ({pk_list}) DO NOTHING"

    return (
        f"INSERT INTO {quote_ident(table)} ({column_list}) VALUES %s {conflict_clause}"
    )


def sync_table(local_conn, neon_conn, table: str) -> None:
    columns, rows = fetch_rows(local_conn, table)
    print(f"[{table}] Fetched {len(rows)} rows from local database.")

    if not rows:
        print(f"[{table}] Nothing to sync.")
        return

    pk_cols = resolve_primary_keys(columns)
    query = build_upsert_sql(table, columns, pk_cols)

    with neon_conn.cursor() as cur:
        total = 0
        for batch in chunked(rows, BATCH_SIZE):
            execute_values(cur, query, batch)
            total += len(batch)
            print(f"[{table}] Synced {total}/{len(rows)} rows...")

    neon_conn.commit()
    print(f"[{table}] Sync complete.")


def main() -> None:
    load_dotenv()

    local_url = require_env("LOCAL_DATABASE_URL")
    neon_url = require_env("NEON_DATABASE_URL")

    local_conn = psycopg2.connect(local_url)
    neon_conn = psycopg2.connect(neon_url)

    try:
        for table in TABLES:
            sync_table(local_conn, neon_conn, table)
    finally:
        local_conn.close()
        neon_conn.close()


if __name__ == "__main__":
    main()
