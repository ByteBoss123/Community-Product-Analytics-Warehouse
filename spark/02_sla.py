"""Dataset SLAs on the Spark lake. Each check is SQL returning one number; results are
appended to lake/ops/sla_results (Parquet) and a breach exits non-zero (fails the DAG task).

Checks read temp views named <layer>_<table>, so tests can swap in a mutated view and
confirm the matching check fires, without rewriting data on disk.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

LAKE = Path(__file__).resolve().parents[1] / "lake"
sys.path.insert(0, str(Path(__file__).resolve().parent))

PII_COLUMNS = {"display_name", "location", "website_url", "user_id"}
ANALYTICS_TABLES = ["fct_user_events", "dim_user", "dim_post", "bridge_question_tag", "agg_monthly_active",
                    "agg_weekly_active", "agg_retention_cohort", "agg_activation", "fct_question_outcomes",
                    "agg_question_funnel_yearly", "agg_tag_health", "fct_post_votes_daily",
                    "fct_experiment_assignment", "fct_experiment_user_metrics", "agg_experiment_results"]

CHECKS = [
    ("analytics.fct_user_events", "freshness", "days between latest event and source snapshot end",
     "SELECT datediff((SELECT CAST(snapshot_end AS DATE) FROM stg_snapshot),"
     " (SELECT max(event_date) FROM analytics_fct_user_events))", "<=", 1),
    ("raw.*", "completeness", "landed rows minus source rows, all 8 tables (from ingest audit)",
     "SELECT {ingest_gap}", "==", 0),
    ("stg.votes", "completeness", "raw votes minus (staged + quarantined)",
     "SELECT (SELECT count(*) FROM raw_votes) - (SELECT count(*) FROM stg_votes)"
     " - (SELECT count(*) FROM quarantine_votes_orphan_post)", "==", 0),
    ("analytics.fct_user_events", "completeness", "owned questions+answers missing from event log",
     "SELECT count(*) FROM stg_posts p LEFT ANTI JOIN analytics_fct_user_events e"
     " ON e.event_id = concat('post:', p.post_id)"
     " WHERE p.post_type IN ('question','answer') AND p.owner_user_id IS NOT NULL", "==", 0),
    ("analytics.fct_user_events", "completeness", "attributed comments missing from event log",
     "SELECT count(*) FROM stg_comments c LEFT ANTI JOIN analytics_fct_user_events e"
     " ON e.event_id = concat('comment:', c.comment_id) WHERE c.user_id IS NOT NULL", "==", 0),
    ("analytics.fct_user_events", "uniqueness", "duplicate event_id rows",
     "SELECT count(*) - count(DISTINCT event_id) FROM analytics_fct_user_events", "==", 0),
    ("analytics.dim_user", "uniqueness", "duplicate user_key rows",
     "SELECT count(*) - count(DISTINCT user_key) FROM analytics_dim_user", "==", 0),
    ("analytics.fct_user_events", "integrity", "events whose user_key is not in dim_user",
     "SELECT count(*) FROM analytics_fct_user_events e LEFT ANTI JOIN analytics_dim_user u"
     " ON e.user_key = u.user_key", "==", 0),
    ("analytics.fct_question_outcomes", "integrity", "answers whose parent question is missing",
     "SELECT count(*) FROM stg_posts a LEFT ANTI JOIN stg_posts q ON q.post_id = a.parent_post_id"
     " WHERE a.post_type = 'answer'", "==", 0),
    ("analytics.bridge_question_tag", "integrity", "question tags not in the Tags table",
     "SELECT count(*) FROM analytics_bridge_question_tag b LEFT ANTI JOIN raw_tags t ON t.TagName = b.tag",
     "==", 0),
    ("analytics.fct_question_outcomes", "validity", "negative time-to-first-answer rows",
     "SELECT count(*) FROM analytics_fct_question_outcomes WHERE hours_to_first_answer < 0", "==", 0),
    ("analytics.fct_question_outcomes", "validity", "answers predating their question outside merged questions",
     "SELECT count(*) FROM analytics_fct_question_outcomes WHERE answers_predating_question > 0"
     " AND NOT is_merge_destination", "==", 0),
    ("analytics.agg_retention_cohort", "validity", "retention rates outside [0,1] or month-0 not 1.0",
     "SELECT count(*) FROM analytics_agg_retention_cohort WHERE retention_rate < 0 OR retention_rate > 1"
     " OR (month_offset = 0 AND retention_rate <> 1)", "==", 0),
    ("analytics.agg_retention_cohort", "completeness", "cohort-month offsets missing from the retention grid",
     "SELECT count(*) FROM (SELECT cohort_month, count(*) n, max(month_offset) mx"
     " FROM analytics_agg_retention_cohort GROUP BY 1) WHERE n <> mx + 1", "==", 0),
    ("analytics.agg_experiment_results", "validity", "experiment sample-ratio mismatch (min SRM p-value; must be >= 0.001)",
     "SELECT coalesce(min(srm_p_value), 0) FROM analytics_agg_experiment_results", ">=", 0.001),
    ("analytics.*", "privacy", "PII / raw-id columns present in analytics tables", "SELECT {pii_count}", "==", 0),
]
OPS = {"<=": lambda a, b: a <= b, "==": lambda a, b: a == b, ">=": lambda a, b: a >= b}


def register_all(spark):
    for layer, tables in {
        "raw": ["votes", "tags"],
        "stg": ["snapshot", "posts", "votes", "comments"],
        "quarantine": ["votes_orphan_post"],
        "analytics": ANALYTICS_TABLES,
    }.items():
        for t in tables:
            path = LAKE / layer / (f"{t}.parquet" if layer == "raw" else t)
            spark.read.parquet(str(path)).createOrReplaceTempView(f"{layer}_{t}")


def run(spark, ingest_audit=None):
    audit = ingest_audit or json.loads((LAKE / "ops" / "ingest_audit.json").read_text())
    ingest_gap = sum(abs(v["rows"] - v["source_rows"]) for v in audit.values())
    pii = sum(len(PII_COLUMNS & set(spark.table(f"analytics_{t}").columns)) for t in ANALYTICS_TABLES)
    results = []
    for dataset, kind, desc, sql, op, thr in CHECKS:
        observed = spark.sql(sql.format(ingest_gap=ingest_gap, pii_count=pii)).collect()[0][0]
        results.append((dataset, kind, desc, float(observed), op, float(thr), OPS[op](observed, thr)))
    return results


def main():
    from importlib import import_module
    spark = import_module("01_transform").spark_session()
    register_all(spark)
    results = run(spark)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = spark.createDataFrame([(now, *r) for r in results],
                               "checked_at timestamp, dataset string, sla_type string, description string,"
                               " observed double, comparator string, threshold double, passed boolean")
    df.write.mode("append").parquet(str(LAKE / "ops" / "sla_results"))
    for r in results:
        print(f"{'PASS' if r[6] else 'BREACH':6s} {r[1]:12s} {r[2]:62s} observed={r[3]:g} {r[4]} {r[5]:g}")
    breaches = sum(not r[6] for r in results)
    print(f"{len(results) - breaches}/{len(results)} SLAs met")
    spark.stop()
    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
