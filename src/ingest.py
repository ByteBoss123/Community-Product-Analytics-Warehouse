"""Stage 1: parse the raw Stack Exchange XML dump into typed raw tables in DuckDB.

Every raw file is checksummed against data/SHA256SUMS before load, and an
ingest audit row (file, sha256, rows parsed) is written for reconciliation.
"""
import hashlib
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
DB = ROOT / "data" / "warehouse.duckdb"

# Columns kept per source table, with target dtypes. Anything not listed is dropped
# at the boundary (e.g. Body/Text free text is kept only where the spec needs it).
SCHEMAS = {
    "Posts": {"Id": "int", "PostTypeId": "int", "ParentId": "int", "AcceptedAnswerId": "int",
              "CreationDate": "ts", "Score": "int", "ViewCount": "int", "OwnerUserId": "int",
              "Tags": "str", "AnswerCount": "int", "CommentCount": "int", "ClosedDate": "ts",
              "Title": "str"},
    "Comments": {"Id": "int", "PostId": "int", "Score": "int", "CreationDate": "ts", "UserId": "int"},
    "Votes": {"Id": "int", "PostId": "int", "VoteTypeId": "int", "UserId": "int",
              "CreationDate": "ts", "BountyAmount": "int"},
    "PostHistory": {"Id": "int", "PostHistoryTypeId": "int", "PostId": "int",
                    "CreationDate": "ts", "UserId": "int"},
    "Badges": {"Id": "int", "UserId": "int", "Name": "str", "Date": "ts", "Class": "int",
               "TagBased": "str"},
    "Users": {"Id": "int", "Reputation": "int", "CreationDate": "ts", "DisplayName": "str",
              "LastAccessDate": "ts", "Location": "str", "WebsiteUrl": "str", "AboutMe": "str",
              "Views": "int", "UpVotes": "int", "DownVotes": "int", "AccountId": "int"},
    "Tags": {"Id": "int", "TagName": "str", "Count": "int"},
    "PostLinks": {"Id": "int", "CreationDate": "ts", "PostId": "int", "RelatedPostId": "int",
                  "LinkTypeId": "int"},
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def expected_checksums() -> dict:
    out = {}
    for line in (ROOT / "data" / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split()
        out[name] = digest
    return out


def parse(name: str) -> pd.DataFrame:
    cols = SCHEMAS[name]
    rows = [{c: el.attrib.get(c) for c in cols}
            for el in ET.parse(RAW / f"{name}.xml").getroot().iter("row")]
    df = pd.DataFrame(rows, columns=list(cols))
    for c, t in cols.items():
        if t == "int":
            df[c] = pd.to_numeric(df[c]).astype("Int64")
        elif t == "ts":
            df[c] = pd.to_datetime(df[c])
    return df


def main() -> int:
    expected = expected_checksums()
    con = duckdb.connect(str(DB))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("""CREATE OR REPLACE TABLE ops.ingest_audit(
        file VARCHAR, sha256 VARCHAR, checksum_ok BOOLEAN, rows_parsed BIGINT, loaded_at TIMESTAMP)""")
    failed = False
    for name in SCHEMAS:
        path = RAW / f"{name}.xml"
        digest = sha256(path)
        ok = digest == expected.get(path.name)
        if not ok:
            failed = True
            print(f"CHECKSUM MISMATCH {path.name}", file=sys.stderr)
            continue
        df = parse(name)
        con.register("df", df)
        con.execute(f"CREATE OR REPLACE TABLE raw.{name.lower()} AS SELECT * FROM df")
        con.unregister("df")
        con.execute("INSERT INTO ops.ingest_audit VALUES (?,?,?,?,?)",
                    [path.name, digest, ok, len(df), datetime.now(timezone.utc).replace(tzinfo=None)])
        print(f"{name:12s} {len(df):>6,} rows")
    con.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
