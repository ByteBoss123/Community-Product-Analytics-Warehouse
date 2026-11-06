"""Stage 4: dataset SLAs. Each check returns (observed, threshold, passed) and is
logged to ops.sla_results so breaches are queryable and alertable.

SLA owner for every dataset below: product-analytics data engineering.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DB = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"

PII_COLUMNS = {"display_name", "location", "website_url", "about_me", "user_id"}

# (dataset, sla_type, description, sql returning one numeric value, comparator, threshold)
CHECKS = [
    # Freshness: the fact table must reach the source's latest day (no truncated loads).
    ("analytics.fct_user_events", "freshness",
     "days between latest event and source snapshot end",
     "SELECT date_diff('day', (SELECT max(event_date) FROM analytics.fct_user_events),"
     " (SELECT snapshot_end::DATE FROM stg.snapshot))", "<=", 1),
    # Completeness: every raw vote is either staged or quarantined (100% accounted for).
    ("stg.votes", "completeness",
     "raw votes minus (staged + quarantined)",
     "SELECT (SELECT count(*) FROM raw.votes) - (SELECT count(*) FROM stg.votes)"
     " - (SELECT count(*) FROM quarantine.votes_orphan_post)", "==", 0),
    # Completeness: every attributable question/answer reaches the event log.
    ("analytics.fct_user_events", "completeness",
     "owned questions+answers missing from event log",
     "SELECT count(*) FROM stg.posts p WHERE post_type IN ('question','answer')"
     " AND owner_user_id IS NOT NULL AND 'post:' || post_id NOT IN"
     " (SELECT event_id FROM analytics.fct_user_events)", "==", 0),
    ("analytics.fct_user_events", "completeness",
     "attributed comments missing from event log",
     "SELECT count(*) FROM stg.comments WHERE user_id IS NOT NULL AND 'comment:' || comment_id"
     " NOT IN (SELECT event_id FROM analytics.fct_user_events)", "==", 0),
    # Uniqueness of grain.
    ("analytics.fct_user_events", "uniqueness", "duplicate event_id rows",
     "SELECT count(*) - count(DISTINCT event_id) FROM analytics.fct_user_events", "==", 0),
    ("analytics.dim_user", "uniqueness", "duplicate user_key rows",
     "SELECT count(*) - count(DISTINCT user_key) FROM analytics.dim_user", "==", 0),
    # Referential integrity.
    ("analytics.fct_user_events", "integrity", "events whose user_key is not in dim_user",
     "SELECT count(*) FROM analytics.fct_user_events e LEFT JOIN analytics.dim_user u"
     " USING (user_key) WHERE u.user_key IS NULL", "<=", 0),
    ("analytics.fct_question_outcomes", "integrity", "answers whose parent question is missing",
     "SELECT count(*) FROM stg.posts a LEFT JOIN stg.posts q ON q.post_id = a.parent_post_id"
     " WHERE a.post_type = 'answer' AND q.post_id IS NULL", "==", 0),
    # Validity.
    ("analytics.fct_question_outcomes", "validity", "negative time-to-first-answer rows",
     "SELECT count(*) FROM analytics.fct_question_outcomes WHERE hours_to_first_answer < 0", "==", 0),
    ("analytics.agg_retention_cohort", "validity", "retention rates outside [0,1]",
     "SELECT count(*) FROM analytics.agg_retention_cohort WHERE retention_rate < 0 OR retention_rate > 1",
     "==", 0),
    ("analytics.agg_retention_cohort", "validity", "week-0 retention not equal to 1.0",
     "SELECT count(*) FROM analytics.agg_retention_cohort WHERE week_offset = 0 AND retention_rate <> 1",
     "==", 0),
    ("analytics.agg_retention_cohort", "completeness",
     "cohort-week offsets missing from the retention grid",
     "SELECT count(*) FROM (SELECT cohort_week, count(*) n, max(week_offset) mx"
     " FROM analytics.agg_retention_cohort GROUP BY 1) WHERE n <> mx + 1", "==", 0),
    # Privacy: analytics schema must expose no PII or raw user ids.
    ("analytics.*", "privacy", "PII / raw-id columns present in analytics schema",
     "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'analytics'"
     " AND column_name IN (" + ",".join(f"'{c}'" for c in sorted(PII_COLUMNS)) + ")", "==", 0),
]

OPS = {"<=": lambda a, b: a <= b, "==": lambda a, b: a == b, ">=": lambda a, b: a >= b}


def run(con) -> list:
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("""CREATE TABLE IF NOT EXISTS ops.sla_results(
        checked_at TIMESTAMP, dataset VARCHAR, sla_type VARCHAR, description VARCHAR,
        observed DOUBLE, comparator VARCHAR, threshold DOUBLE, passed BOOLEAN)""")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    results = []
    for dataset, kind, desc, sql, op, thr in CHECKS:
        observed = con.execute(sql).fetchone()[0]
        passed = OPS[op](observed, thr)
        con.execute("INSERT INTO ops.sla_results VALUES (?,?,?,?,?,?,?,?)",
                    [now, dataset, kind, desc, observed, op, thr, passed])
        results.append((dataset, kind, desc, observed, op, thr, passed))
    return results


def main() -> int:
    con = duckdb.connect(str(DB))
    results = run(con)
    con.close()
    for r in results:
        print(f"{'PASS' if r[6] else 'BREACH':6s} {r[1]:12s} {r[2]:60s} observed={r[3]} {r[4]} {r[5]}")
    breaches = sum(not r[6] for r in results)
    print(f"{len(results) - breaches}/{len(results)} SLAs met")
    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
