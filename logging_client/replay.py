"""Replay the real Cross Validated history through the v2 logging client, in event-time order.

This exercises the logger on 3.2M real actions and measures how much of the historical
logging would fail the v2 contract (the gap analysis in docs/logging_spec.md, quantified).
Output: lake/logs/events (accepted, JSONL partitioned by date), lake/logs/dead_letter,
lake/ops/logging_replay.json (counts).
"""
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))
from logger import EventLogger  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "lake"
SALT = "cpa-salt-v1:"

SQL = f"""
CREATE MACRO ukey(id) AS CASE WHEN id IS NULL THEN NULL ELSE substr(sha256('{SALT}' || id::VARCHAR), 1, 16) END;
CREATE MACRO ts_ms(t) AS strftime(t, '%Y-%m-%dT%H:%M:%S.') || lpad((epoch_ms(t) % 1000)::VARCHAR, 3, '0') || 'Z';
CREATE VIEW posts AS SELECT * FROM read_parquet('{LAKE}/stg/posts/*.parquet');
CREATE VIEW votes AS SELECT * FROM read_parquet('{LAKE}/stg/votes/*.parquet');
SELECT * FROM (
  SELECT 'post:' || post_id AS event_id, 'question_posted' AS event_name, created_at AS ts, ukey(owner_user_id) AS actor_key,
         post_id, NULL::BIGINT AS question_id,
         len(regexp_extract_all(tags_raw, '[^|<>]+')) AS tag_count, NULL AS kind
  FROM posts WHERE post_type = 'question'
  UNION ALL
  SELECT 'post:' || post_id, 'answer_posted', created_at, ukey(owner_user_id), post_id, parent_post_id, NULL, NULL
  FROM posts WHERE post_type = 'answer'
  UNION ALL
  SELECT 'comment:' || comment_id, 'comment_posted', created_at, ukey(user_id), post_id, NULL, NULL, NULL
  FROM read_parquet('{LAKE}/stg/comments/*.parquet')
  UNION ALL
  SELECT 'edit:' || history_id, 'post_edited', created_at, ukey(user_id), post_id, NULL, NULL,
         CASE history_type_id WHEN 4 THEN 'title' WHEN 5 THEN 'body' ELSE 'tags' END
  FROM read_parquet('{LAKE}/stg/post_edits/*.parquet')
  UNION ALL
  SELECT 'vote:' || v.vote_id, 'answer_accepted', v.vote_date::TIMESTAMP, ukey(q.owner_user_id), v.post_id, q.post_id, NULL, NULL
  FROM votes v JOIN posts a ON a.post_id = v.post_id AND a.post_type = 'answer'
  JOIN posts q ON q.post_id = a.parent_post_id WHERE v.vote_type = 'accept'
  UNION ALL
  SELECT 'vote:' || vote_id, 'vote_cast', vote_date::TIMESTAMP, ukey(user_id), post_id, NULL, NULL, vote_type
  FROM votes WHERE vote_type IN ('upvote', 'downvote', 'favorite', 'bounty_start')
) ORDER BY ts, event_id
"""


def to_event(r):
    event_id, name, ts, actor, post_id, question_id, tag_count, kind = r
    e = {"event_id": event_id, "event_name": name, "schema_version": 2, "event_ts": ts,
         "actor_key": actor, "client": "replay", "post_id": post_id}
    if name == "question_posted":
        e["tag_count"] = tag_count
    elif name in ("answer_posted", "answer_accepted"):
        e["question_id"] = question_id
    elif name == "post_edited":
        e["edit_kind"] = kind
    elif name == "vote_cast":
        e["vote_kind"] = kind
    # JSON has no "null means absent": drop None so the contract reports a missing required field
    return {k: v for k, v in e.items() if v is not None}


def main():
    sink = LAKE / "logs"
    if sink.exists():
        import shutil
        shutil.rmtree(sink)
    con = duckdb.connect()
    stmts = [s for s in SQL.split(";") if s.strip()]
    for s in stmts[:-1]:
        con.execute(s)
    cur = con.execute(stmts[-1].replace("SELECT * FROM (", "SELECT event_id, event_name, ts_ms(ts), actor_key, post_id, question_id, tag_count, kind FROM (", 1))
    log, t0, n = EventLogger(sink, flush_every=200_000), time.time(), 0
    while rows := cur.fetchmany(100_000):
        for r in rows:
            log.log(to_event(r))
        n += len(rows)
    log.flush()
    stats = {**log.stats(), "replayed": n, "seconds": round(time.time() - t0, 1)}
    (LAKE / "ops" / "logging_replay.json").write_text(json.dumps(stats, indent=1))
    print(json.dumps(stats, indent=1))


if __name__ == "__main__":
    main()
