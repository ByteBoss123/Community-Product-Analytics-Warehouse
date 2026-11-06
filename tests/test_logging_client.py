import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "logging_client"))
from logger import EventLogger  # noqa: E402

GOOD = {"event_id": "post:42", "event_name": "answer_posted", "schema_version": 2,
        "event_ts": "2023-05-01T12:00:00.123Z", "actor_key": "0123456789abcdef",
        "client": "web", "post_id": 42, "question_id": 7}


def fresh(tmp_path):
    return EventLogger(tmp_path, flush_every=10)


def test_valid_event_is_emitted_and_written(tmp_path):
    log = fresh(tmp_path)
    assert log.log(dict(GOOD))
    log.flush()
    files = list((tmp_path / "events" / "event_date=2023-05-01").glob("*.jsonl.gz"))
    assert len(files) == 1
    assert json.loads(gzip.open(files[0], "rt").readline())["event_id"] == "post:42"


def test_missing_actor_goes_to_dead_letter(tmp_path):
    log = fresh(tmp_path)
    e = dict(GOOD); e.pop("actor_key")
    assert not log.log(e)
    log.flush()
    dead = gzip.open(next((tmp_path / "dead_letter").glob("*.gz")), "rt").readline()
    assert "actor_key" in json.loads(dead)["reason"]
    assert not (tmp_path / "events").exists()


def test_pii_field_not_in_contract_is_rejected(tmp_path):
    e = dict(GOOD, display_name="Jane Doe")
    assert not fresh(tmp_path).log(e)


def test_raw_user_id_as_actor_is_rejected(tmp_path):
    assert not fresh(tmp_path).log(dict(GOOD, actor_key="12345"))


def test_second_precision_timestamp_is_rejected(tmp_path):
    assert not fresh(tmp_path).log(dict(GOOD, event_ts="2023-05-01T12:00:00Z"))


def test_wrong_type_and_unknown_event_are_rejected(tmp_path):
    log = fresh(tmp_path)
    assert not log.log(dict(GOOD, post_id="42"))
    assert not log.log(dict(GOOD, event_name="page_viewed"))
    assert log.stats()["rejected_total"] == 2


def test_duplicate_event_id_is_dropped_once(tmp_path):
    log = fresh(tmp_path)
    assert log.log(dict(GOOD)) and not log.log(dict(GOOD))
    assert log.stats()["emitted_total"] == 1 and log.stats()["duplicates_dropped"] == 1


def test_replay_reconciles_with_warehouse():
    rec_file = ROOT / "lake" / "ops" / "log_reconciliation.json"
    if not rec_file.exists():
        import pytest
        pytest.skip("run logging_client/replay.py and spark/04_ingest_logs.py first")
    rec = json.loads(rec_file.read_text())
    assert rec["logged_rows"] == rec["warehouse_rows"]
    assert rec["logged_not_in_warehouse"] == rec["warehouse_not_in_logged"] == 0
    assert rec["name_or_actor_mismatches"] == rec["timestamp_mismatches"] == 0
