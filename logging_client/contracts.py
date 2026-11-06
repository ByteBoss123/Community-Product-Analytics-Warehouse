"""Event contracts for the community Q&A product (schema v2).

Every event shares an envelope; each event_name adds required properties. Contracts are
JSON Schema with additionalProperties=false, so a field that is not in the contract (for
example a display name) is rejected rather than silently logged.

v2 changes vs. the historical source logging (see docs/logging_spec.md, section 3):
  * vote_cast requires actor_key (historically votes carried no voter for up/down votes)
  * timestamps must be millisecond ISO-8601 (historically votes were day-grain)
"""
import json
from pathlib import Path

SCHEMA_VERSION = 2
KEY = {"type": "string", "pattern": "^[0-9a-f]{16}$"}          # salted-hash pseudonymous key
TS_MS = {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"}
POST_ID = {"type": "integer", "minimum": 1}

ENVELOPE = {
    "event_id": {"type": "string", "minLength": 3},
    "event_name": {"type": "string"},
    "schema_version": {"const": SCHEMA_VERSION},
    "event_ts": TS_MS,
    "actor_key": KEY,
    "client": {"enum": ["web", "ios", "android", "api", "replay"]},
}

PROPERTIES = {
    "question_posted": {"post_id": POST_ID, "tag_count": {"type": "integer", "minimum": 1, "maximum": 6}},
    "answer_posted": {"post_id": POST_ID, "question_id": POST_ID},
    "comment_posted": {"post_id": POST_ID},
    "post_edited": {"post_id": POST_ID, "edit_kind": {"enum": ["title", "body", "tags"]}},
    "answer_accepted": {"post_id": POST_ID, "question_id": POST_ID},
    "vote_cast": {"post_id": POST_ID, "vote_kind": {"enum": ["upvote", "downvote", "favorite", "bounty_start"]}},
}


def schema_for(event_name):
    props = {**ENVELOPE, **{k: v for k, v in PROPERTIES[event_name].items()}}
    props["event_name"] = {"const": event_name}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": event_name,
        "type": "object",
        "properties": props,
        "required": sorted(props),
        "additionalProperties": False,
    }


def write_contracts(out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name in PROPERTIES:
        (out / f"{name}.v{SCHEMA_VERSION}.json").write_text(json.dumps(schema_for(name), indent=2))


if __name__ == "__main__":
    write_contracts(Path(__file__).parent / "contracts")
