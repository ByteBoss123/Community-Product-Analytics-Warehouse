"""Ingest the logging client's JSONL sink with Spark and reconcile it against the warehouse.

lake/logs/events/event_date=YYYY-MM-DD/*.jsonl.gz  ->  lake/analytics/fct_logged_events (by event_year)
Reconciliation: every logged event_id must exist in fct_user_events and vice versa (anti-joins
both ways = 0), and per-event-name counts must match. Result -> lake/ops/log_reconciliation.json.
"""
import json
import sys
from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark"))
LAKE = ROOT / "lake"

SCHEMA = ("event_id string, event_name string, schema_version int, event_ts string, actor_key string, "
          "client string, post_id long, question_id long, tag_count int, edit_kind string, vote_kind string")

# vote_cast events accepted by the v2 contract are favorites and bounty starts, which the
# warehouse models as their own event names.
WAREHOUSE_NAME = "CASE WHEN event_name = 'vote_cast' THEN vote_kind ELSE event_name END"


def main():
    spark = import_module("01_transform").spark_session()
    logs = spark.read.schema(SCHEMA).json(str(LAKE / "logs" / "events"))
    logs.createOrReplaceTempView("logs")
    spark.sql(f"""
        SELECT event_id, {WAREHOUSE_NAME} AS event_name, actor_key AS user_key,
               to_timestamp(event_ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS event_ts, post_id,
               question_id, tag_count, edit_kind, schema_version, client,
               year(to_timestamp(event_ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) AS event_year
        FROM logs""").write.mode("overwrite").partitionBy("event_year").parquet(str(LAKE / "analytics" / "fct_logged_events"))
    spark.read.parquet(str(LAKE / "analytics" / "fct_logged_events")).createOrReplaceTempView("logged")
    spark.read.parquet(str(LAKE / "analytics" / "fct_user_events")).createOrReplaceTempView("wh")
    one = lambda q: spark.sql(q).collect()[0][0]
    rec = {
        "logged_rows": one("SELECT count(*) FROM logged"),
        "warehouse_rows": one("SELECT count(*) FROM wh"),
        "logged_not_in_warehouse": one("SELECT count(*) FROM logged l LEFT ANTI JOIN wh w ON l.event_id = w.event_id"),
        "warehouse_not_in_logged": one("SELECT count(*) FROM wh w LEFT ANTI JOIN logged l ON l.event_id = w.event_id"),
        "name_or_actor_mismatches": one("""SELECT count(*) FROM logged l JOIN wh w ON l.event_id = w.event_id
                                           WHERE l.event_name <> w.event_name OR l.user_key <> w.user_key"""),
        "timestamp_mismatches": one("""SELECT count(*) FROM logged l JOIN wh w ON l.event_id = w.event_id
                                       WHERE l.event_ts <> w.event_ts"""),
        "partitions": one("SELECT count(DISTINCT event_year) FROM logged"),
    }
    (LAKE / "ops" / "log_reconciliation.json").write_text(json.dumps(rec, indent=1))
    print(json.dumps(rec, indent=1))
    spark.stop()


if __name__ == "__main__":
    main()
