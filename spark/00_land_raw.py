"""Land the Cross Validated (stats.stackexchange.com) dump as typed raw Parquet.

Inputs (in DUMP_DIR):
  Votes.xml, Users.xml, Badges.xml, Tags.xml, PostLinks.xml   original dump XML
  compact/{posts,comments,posthistory}/part*.tsv.gz           column-projected extracts of
      Posts.xml / Comments.xml / PostHistory.xml (free text dropped on the source machine;
      produced by spark/compact_extract.py, one TSV row per XML <row>)
Output: lake/raw/<table>.parquet and lake/ops/ingest_audit.json (row counts + SHA-256 per input).
"""
import gzip
import hashlib
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DUMP = Path(sys.argv[1] if len(sys.argv) > 1 else "/home/claude/stats_dump")
LAKE = Path(__file__).resolve().parents[1] / "lake"

I, T, S = "i", "t", "s"
XML_TABLES = {
    "votes": ("Votes.xml", {"Id": I, "PostId": I, "VoteTypeId": I, "UserId": I, "CreationDate": T, "BountyAmount": I}),
    "users": ("Users.xml", {"Id": I, "Reputation": I, "CreationDate": T, "DisplayName": S, "LastAccessDate": T,
                            "Location": S, "WebsiteUrl": S, "Views": I, "UpVotes": I, "DownVotes": I, "AccountId": I}),
    "badges": ("Badges.xml", {"Id": I, "UserId": I, "Name": S, "Date": T, "Class": I, "TagBased": S}),
    "tags": ("Tags.xml", {"Id": I, "TagName": S, "Count": I}),
    "postlinks": ("PostLinks.xml", {"Id": I, "CreationDate": T, "PostId": I, "RelatedPostId": I, "LinkTypeId": I}),
}
TSV_TABLES = {
    "posts": {"Id": I, "PostTypeId": I, "ParentId": I, "AcceptedAnswerId": I, "CreationDate": T, "Score": I,
              "ViewCount": I, "OwnerUserId": I, "Tags": S, "AnswerCount": I, "CommentCount": I, "ClosedDate": T},
    "comments": {"Id": I, "PostId": I, "Score": I, "CreationDate": T, "UserId": I},
    "posthistory": {"Id": I, "PostHistoryTypeId": I, "PostId": I, "CreationDate": T, "UserId": I},
}
ARROW = {I: pa.int64(), T: pa.timestamp("ms"), S: pa.string()}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def to_table(cols, rows):
    arrays = []
    for c, t in cols.items():
        vals = [r.get(c) or None for r in rows]
        if t == I:
            arrays.append(pa.array([int(v) if v is not None else None for v in vals], pa.int64()))
        elif t == T:
            arrays.append(pa.array([v[:23] if v else None for v in vals], pa.string()).cast(pa.timestamp("ms")))
        else:
            arrays.append(pa.array(vals, pa.string()))
    return pa.Table.from_arrays(arrays, schema=pa.schema([(c, ARROW[t]) for c, t in cols.items()]))


def write(name, cols, row_iter, batch=250_000):
    out = LAKE / "raw" / f"{name}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    writer, buf, n = None, [], 0
    for r in row_iter:
        buf.append(r)
        if len(buf) >= batch:
            t = to_table(cols, buf)
            writer = writer or pq.ParquetWriter(out, t.schema, compression="zstd")
            writer.write_table(t); n += len(buf); buf = []
    t = to_table(cols, buf)
    writer = writer or pq.ParquetWriter(out, t.schema, compression="zstd")
    writer.write_table(t); n += len(buf)
    writer.close()
    return n


def xml_rows(path, cols):
    for _, el in ET.iterparse(path):
        if el.tag == "row":
            yield {c: el.attrib.get(c) for c in cols}
        el.clear()


def tsv_rows(files, cols):
    names = list(cols)
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8") as g:
            for line in g:
                vals = line.rstrip("\n").split("\t")
                assert len(vals) == len(names), (f, line[:200])
                yield dict(zip(names, vals))


def main():
    audit = {}
    for name, (fname, cols) in XML_TABLES.items():
        p = DUMP / fname
        n = write(name, cols, xml_rows(p, cols))
        audit[name] = {"source": fname, "sha256": sha256(p), "rows": n,
                       "source_rows": p.read_text(encoding="utf-8-sig").count("<row ")}
        print(name, n)
    for name, cols in TSV_TABLES.items():
        files = sorted((DUMP / "compact" / name).glob("part*.tsv.gz"))
        n = write(name, cols, tsv_rows(files, cols))
        source_rows = sum(1 for f in files for _ in gzip.open(f, "rt"))
        audit[name] = {"source": f"compact/{name}", "sha256": [sha256(f) for f in files],
                       "rows": n, "source_rows": source_rows}
        print(name, n)
    (LAKE / "ops").mkdir(parents=True, exist_ok=True)
    (LAKE / "ops" / "ingest_audit.json").write_text(json.dumps(audit, indent=1))


if __name__ == "__main__":
    main()
