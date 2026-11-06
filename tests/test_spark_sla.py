"""Spark SLA tests: clean lake passes; each injected defect trips exactly its check.
Defects are injected by replacing temp views, so the lake on disk is never modified."""
import sys
from importlib import import_module
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark"))
LAKE = ROOT / "lake"

pytestmark = pytest.mark.skipif(not (LAKE / "analytics").exists(), reason="run the spark pipeline first")


@pytest.fixture(scope="module")
def spark():
    s = import_module("01_transform").spark_session()
    yield s
    s.stop()


@pytest.fixture()
def sla(spark):
    mod = import_module("02_sla")
    mod.register_all(spark)
    yield mod
    mod.register_all(spark)  # restore clean views


def breached(results, kind, fragment):
    return [r for r in results if not r[6] and r[1] == kind and fragment in r[2]]


def test_clean_lake_meets_all_slas(spark, sla):
    assert all(r[6] for r in sla.run(spark))


def test_duplicate_event_breaches_uniqueness(spark, sla):
    spark.sql("SELECT * FROM analytics_fct_user_events UNION ALL "
              "(SELECT * FROM analytics_fct_user_events LIMIT 1)").createOrReplaceTempView("analytics_fct_user_events")
    assert breached(sla.run(spark), "uniqueness", "duplicate event_id")


def test_pii_column_breaches_privacy(spark, sla):
    spark.sql("SELECT *, 'x' AS display_name FROM analytics_dim_user").createOrReplaceTempView("analytics_dim_user")
    assert breached(sla.run(spark), "privacy", "PII")


def test_truncated_load_breaches_freshness(spark, sla):
    spark.sql("SELECT * FROM analytics_fct_user_events WHERE event_date < DATE '2024-03-01'") \
        .createOrReplaceTempView("analytics_fct_user_events")
    assert breached(sla.run(spark), "freshness", "latest event")


def test_dropped_vote_breaches_completeness(spark, sla):
    spark.sql("SELECT * FROM stg_votes WHERE vote_id <> (SELECT min(vote_id) FROM stg_votes)") \
        .createOrReplaceTempView("stg_votes")
    assert breached(sla.run(spark), "completeness", "raw votes")


def test_unmerged_early_answer_breaches_validity(spark, sla):
    spark.sql("SELECT question_id, created_at, false AS is_merge_destination, 1 AS answers_predating_question, "
              "asked_month, asked_year, answers, first_answer_at, hours_to_first_answer, has_accepted_answer, "
              "is_closed, score, view_count, is_mature FROM analytics_fct_question_outcomes") \
        .createOrReplaceTempView("analytics_fct_question_outcomes")
    assert breached(sla.run(spark), "validity", "predating")


def test_ingest_row_loss_breaches_completeness(spark, sla):
    import json
    audit = json.loads((LAKE / "ops" / "ingest_audit.json").read_text())
    audit["votes"]["rows"] -= 1
    assert breached(sla.run(spark, ingest_audit=audit), "completeness", "landed rows")
