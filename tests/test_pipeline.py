"""Tests run against a temp copy of the built warehouse, so breach tests can mutate it."""
import shutil
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import sla  # noqa: E402

DB = ROOT / "data" / "warehouse.duckdb"


@pytest.fixture()
def con(tmp_path):
    if not DB.exists():
        pytest.skip("run src/pipeline.py first")
    copy = tmp_path / "wh.duckdb"
    shutil.copy(DB, copy)
    c = duckdb.connect(str(copy))
    yield c
    c.close()


def breached(results, kind, desc_fragment):
    return any(r[1] == kind and desc_fragment in r[2] and not r[6] for r in results)


def test_all_slas_pass_on_clean_build(con):
    assert all(r[6] for r in sla.run(con))


def test_ingest_counts_match_xml_rows(con):
    for file, rows in con.execute("SELECT file, rows_parsed FROM ops.ingest_audit").fetchall():
        text = (ROOT / "data" / "raw" / file).read_text(encoding="utf-8-sig")
        assert rows == text.count("<row ")


def test_duplicate_event_breaches_uniqueness(con):
    con.execute("INSERT INTO analytics.fct_user_events SELECT * FROM analytics.fct_user_events LIMIT 1")
    assert breached(sla.run(con), "uniqueness", "duplicate event_id")


def test_pii_column_breaches_privacy(con):
    con.execute("ALTER TABLE analytics.dim_user ADD COLUMN display_name VARCHAR")
    assert breached(sla.run(con), "privacy", "PII")


def test_truncated_load_breaches_freshness(con):
    con.execute("DELETE FROM analytics.fct_user_events WHERE event_date > DATE '2016-12-01'")
    assert breached(sla.run(con), "freshness", "latest event")


def test_dropped_vote_breaches_completeness(con):
    con.execute("DELETE FROM stg.votes WHERE vote_id = (SELECT min(vote_id) FROM stg.votes)")
    assert breached(sla.run(con), "completeness", "raw votes")


def test_retention_grid_has_zero_rows():
    c = duckdb.connect(str(DB), read_only=True)
    zeros = c.execute("SELECT count(*) FROM analytics.agg_retention_cohort WHERE retained_users = 0").fetchone()[0]
    c.close()
    assert zeros > 0  # zero-retention cells must be present, not omitted


def test_analytics_user_key_is_not_raw_id():
    c = duckdb.connect(str(DB), read_only=True)
    overlap = c.execute("""SELECT count(*) FROM analytics.dim_user d
        JOIN restricted.user_profile r ON d.user_key = r.user_id::VARCHAR""").fetchone()[0]
    c.close()
    assert overlap == 0
