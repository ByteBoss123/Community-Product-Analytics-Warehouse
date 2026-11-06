"""Run the full pipeline locally: ingest -> staging -> marts -> SLA checks."""
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import ingest  # noqa: E402
import sla  # noqa: E402


def transform(sql_file: str) -> None:
    con = duckdb.connect(str(ingest.DB))
    con.execute((ROOT / "sql" / sql_file).read_text())
    con.close()
    print(f"built {sql_file}")


def main() -> int:
    if ingest.main() != 0:
        return 1
    transform("01_staging.sql")
    transform("02_marts.sql")
    return sla.main()


if __name__ == "__main__":
    sys.exit(main())
