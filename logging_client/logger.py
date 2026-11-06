"""Client-side event logger.

  log(event)  -> validates against the event's contract, drops duplicates (idempotency on
                 event_id), buffers, and flushes JSONL to the sink partitioned by event_date.
  Rejected events go to a dead-letter sink with the validation error, never to the main sink.
  stats()     -> emitted / rejected (by reason) / duplicate counts, for the delivery SLA.
"""
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path

import fastjsonschema

from contracts import PROPERTIES, schema_for

VALIDATORS = {name: fastjsonschema.compile(schema_for(name)) for name in PROPERTIES}


class EventLogger:
    def __init__(self, sink_dir, flush_every=50_000):
        self.sink = Path(sink_dir)
        self.flush_every = flush_every
        self.buf = defaultdict(list)          # event_date -> lines
        self.dead = []
        self.seen = set()
        self.emitted = Counter()
        self.rejected = Counter()
        self.duplicates = 0
        self.part = 0

    def log(self, event):
        name = event.get("event_name")
        validator = VALIDATORS.get(name)
        if validator is None:
            return self._reject(event, "unknown_event_name")
        try:
            validator(event)
        except fastjsonschema.JsonSchemaValueException as e:
            return self._reject(event, f"{name}: {e.message}")
        if event["event_id"] in self.seen:
            self.duplicates += 1
            return False
        self.seen.add(event["event_id"])
        self.buf[event["event_ts"][:10]].append(json.dumps(event, separators=(",", ":")))
        self.emitted[name] += 1
        if sum(map(len, self.buf.values())) >= self.flush_every:
            self.flush()
        return True

    def _reject(self, event, reason):
        # fastjsonschema messages name the rule, not the offending value, so they bucket cleanly
        self.rejected[reason] += 1
        self.dead.append(json.dumps({"reason": reason, "event": event}, separators=(",", ":"), default=str))
        return False

    def flush(self):
        for day, lines in self.buf.items():
            d = self.sink / "events" / f"event_date={day}"
            d.mkdir(parents=True, exist_ok=True)
            with gzip.open(d / f"part-{self.part:05d}.jsonl.gz", "at") as f:
                f.write("\n".join(lines) + "\n")
        self.buf.clear()
        if self.dead:
            d = self.sink / "dead_letter"
            d.mkdir(parents=True, exist_ok=True)
            with gzip.open(d / f"part-{self.part:05d}.jsonl.gz", "at") as f:
                f.write("\n".join(self.dead) + "\n")
            self.dead.clear()
        self.part += 1

    def stats(self):
        return {"emitted": dict(self.emitted), "emitted_total": sum(self.emitted.values()),
                "rejected": dict(self.rejected), "rejected_total": sum(self.rejected.values()),
                "duplicates_dropped": self.duplicates}
