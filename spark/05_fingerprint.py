"""Order-independent fingerprint of every staged/analytics table: row count plus the sum of a
64-bit hash over all columns. Run in two environments (local, EMR) and compare the JSON files:
identical fingerprints mean identical table contents.

Usage: spark-submit 05_fingerprint.py <label>   (writes <lake>/ops/fingerprint_<label>.json)
"""
import json
import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
T = import_module("01_transform")

TABLES = [("stg", t) for t in ["snapshot", "posts", "votes", "comments", "post_edits", "users", "post_tags"]] + \
         [("quarantine", "votes_orphan_post"), ("restricted", "user_profile")] + \
         [("analytics", t) for t in ["dim_user", "dim_post", "bridge_question_tag", "fct_user_events",
                                     "agg_monthly_active", "agg_weekly_active", "agg_retention_cohort",
                                     "agg_activation", "fct_question_outcomes", "agg_question_funnel_yearly",
                                     "agg_tag_health", "fct_post_votes_daily"]]


def main():
    label = sys.argv[1] if len(sys.argv) > 1 else "local"
    spark = T.spark_session()
    out = {}
    for layer, t in TABLES:
        df = spark.read.parquet(T.lake_path(layer, t))
        cols = sorted(df.columns)  # partition column order differs by reader; sort for stability
        row = df.selectExpr("count(*) AS n",
                            f"sum(xxhash64({', '.join(f'`{c}`' for c in cols)})) AS h").collect()[0]
        out[f"{layer}.{t}"] = {"rows": row["n"], "hash_sum": str(row["h"])}
    body = json.dumps(out, indent=1, sort_keys=True)
    spark.sparkContext.parallelize([body], 1).saveAsTextFile(T.lake_path("ops", f"fingerprint_{label}"))
    print(body)
    spark.stop()


if __name__ == "__main__":
    main()
