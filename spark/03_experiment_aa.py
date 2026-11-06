"""A/A validation of the experiment metrics pipeline on real users.

No treatment was applied: users are split by a salted hash of user_key, and outcomes are
their real behaviour. A correct pipeline must show (1) no sample-ratio mismatch, (2) no
significant metric differences beyond chance, and (3) a false-positive rate near alpha
when the split is re-drawn with many different salts.

Population: accounts created in calendar year 2023 (every user has a full 28-day window
before the dump's snapshot end of 2024-03-31).
Outputs:
  lake/analytics/fct_experiment_assignment/   experiment_id, user_key, variant, assigned_at
  lake/analytics/fct_experiment_user_metrics/ per-user 28-day outcomes
  lake/analytics/agg_experiment_results/      per-metric control vs treatment, delta, CI, p-value
  lake/ops/aa_calibration.json                FPR over N re-salted A/A splits
"""
import hashlib
import json
import math
import sys
from importlib import import_module
from pathlib import Path

import numpy as np
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark"))
LAKE = ROOT / "lake"
EXPERIMENT_ID = "aa_onboarding_2023"
SALT = "exp:aa_onboarding_2023:v1"
N_RESALTS = 1000
ALPHA = 0.05
METRICS = [  # name, type
    ("activated_28d", "binary"),       # primary: any attributable contribution within 28 days of signup
    ("asked_28d", "binary"),
    ("answered_28d", "binary"),
    ("events_28d", "count"),           # guardrail: overall activity volume
]


def bucket(user_key, salt):
    return int(hashlib.md5(f"{salt}:{user_key}".encode()).hexdigest()[:8], 16) % 100


def two_prop(x1, n1, x2, n2):
    p1, p2 = x1 / n1, x2 / n2
    p = (x1 + x2) / (n1 + n2)
    se = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    z = (p2 - p1) / se if se else 0.0
    se_d = math.sqrt(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2)
    return p1, p2, p2 - p1, se_d, 2 * stats.norm.sf(abs(z))


def welch(a, b):
    d = b.mean() - a.mean()
    se = math.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b))
    return a.mean(), b.mean(), d, se, stats.ttest_ind(b, a, equal_var=False).pvalue


def analyse(df, variant):
    ctl, trt = df[variant == 0], df[variant == 1]
    rows = []
    for m, kind in METRICS:
        a, b = ctl[m].to_numpy(), trt[m].to_numpy()
        if kind == "binary":
            c_mean, t_mean, d, se, p = two_prop(a.sum(), len(a), b.sum(), len(b))
        else:
            c_mean, t_mean, d, se, p = welch(a.astype(float), b.astype(float))
        rows.append(dict(metric=m, metric_type=kind, control_n=len(a), treatment_n=len(b),
                         control_mean=round(float(c_mean), 6), treatment_mean=round(float(t_mean), 6),
                         delta=round(float(d), 6), ci_low=round(float(d - 1.96 * se), 6),
                         ci_high=round(float(d + 1.96 * se), 6), p_value=round(float(p), 4),
                         significant=bool(p < ALPHA)))
    return rows


def srm_p(n_ctl, n_trt):
    exp = (n_ctl + n_trt) / 2
    return float(stats.chisquare([n_ctl, n_trt], [exp, exp]).pvalue)


def main():
    spark = import_module("01_transform").spark_session()
    spark.read.parquet(str(LAKE / "analytics" / "dim_user")).createOrReplaceTempView("dim_user")
    spark.read.parquet(str(LAKE / "analytics" / "fct_user_events")).createOrReplaceTempView("ev")
    users = spark.sql("""
        SELECT u.user_key, u.signed_up_at,
               CASE WHEN count(e.event_id) > 0 THEN 1 ELSE 0 END AS activated_28d,
               CASE WHEN sum(CASE WHEN e.event_name = 'question_posted' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS asked_28d,
               CASE WHEN sum(CASE WHEN e.event_name = 'answer_posted' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS answered_28d,
               count(e.event_id) AS events_28d
        FROM dim_user u
        LEFT JOIN ev e ON e.user_key = u.user_key AND e.event_ts >= u.signed_up_at
             AND e.event_ts < u.signed_up_at + INTERVAL 28 DAYS
        WHERE u.signed_up_at >= TIMESTAMP '2023-01-01' AND u.signed_up_at < TIMESTAMP '2024-01-01'
        GROUP BY 1, 2""").toPandas()

    keys = users.user_key.tolist()
    variant = np.array([1 if bucket(k, SALT) < 50 else 0 for k in keys])
    users["variant"] = np.where(variant == 1, "treatment", "control")

    # persist assignment + per-user metrics + results
    assign = users[["user_key", "variant", "signed_up_at"]].rename(columns={"signed_up_at": "assigned_at"})
    assign.insert(0, "experiment_id", EXPERIMENT_ID)
    spark.createDataFrame(assign).write.mode("overwrite").parquet(str(LAKE / "analytics" / "fct_experiment_assignment"))
    spark.createDataFrame(users.assign(experiment_id=EXPERIMENT_ID)).write.mode("overwrite") \
        .parquet(str(LAKE / "analytics" / "fct_experiment_user_metrics"))
    results = analyse(users, variant)
    n_ctl, n_trt = int((variant == 0).sum()), int((variant == 1).sum())
    srm = srm_p(n_ctl, n_trt)
    for r in results:
        r.update(experiment_id=EXPERIMENT_ID, srm_p_value=round(srm, 4))
    spark.createDataFrame(results).write.mode("overwrite").parquet(str(LAKE / "analytics" / "agg_experiment_results"))

    # calibration: re-draw the A/A split with N_RESALTS salts
    pvals = {m: [] for m, _ in METRICS}
    srm_fail = 0
    for i in range(N_RESALTS):
        v = np.array([1 if bucket(k, f"aa-cal-{i}") < 50 else 0 for k in keys])
        srm_fail += srm_p(int((v == 0).sum()), int((v == 1).sum())) < 0.001
        for r in analyse(users, v):
            pvals[r["metric"]].append(r["p_value"])
    calib = {
        "experiment_id": EXPERIMENT_ID, "population": "accounts created in 2023", "users": len(keys),
        "control_n": n_ctl, "treatment_n": n_trt, "srm_p_value": round(srm, 4), "alpha": ALPHA,
        "resalts": N_RESALTS, "srm_failures_at_p_lt_0.001": int(srm_fail),
        "false_positive_rate": {m: round(float(np.mean(np.array(p) < ALPHA)), 4) for m, p in pvals.items()},
        "fpr_95ci_halfwidth": round(1.96 * math.sqrt(ALPHA * (1 - ALPHA) / N_RESALTS), 4),
        "pvalue_uniformity_ks_p": {m: round(float(stats.kstest(p, "uniform").pvalue), 4) for m, p in pvals.items()},
        "results_canonical_split": results,
    }
    (LAKE / "ops" / "aa_calibration.json").write_text(json.dumps(calib, indent=1))
    print(json.dumps({k: v for k, v in calib.items() if k != "results_canonical_split"}, indent=1))
    for r in results:
        print(r["metric"], r["control_mean"], r["treatment_mean"], r["delta"], r["p_value"])
    spark.stop()


if __name__ == "__main__":
    main()
