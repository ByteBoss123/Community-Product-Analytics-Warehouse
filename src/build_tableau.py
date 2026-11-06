"""Build Tableau-ready CSVs from the Spark lake and a .twb workbook that reads them.

Usage: python3 src/build_tableau.py <windows_dir_for_csvs>
Writes tableau/*.csv and tableau/Community_Product_Analytics.twb.
"""
import sys
import uuid
from pathlib import Path
from xml.sax.saxutils import quoteattr

import duckdb

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "tableau"
WIN_DIR = sys.argv[1] if len(sys.argv) > 1 else "C:/Users/amarg/Documents/community-product-analytics/tableau"

# ---------------------------------------------------------------- CSV extracts (from the Spark lake)
LAKE = ROOT / "lake"
EXTRACTS = {
    "monthly_active_long": """
        SELECT month_start, 'Returning' AS segment, returning_contributors AS contributors FROM a.agg_monthly_active
        WHERE month_start >= DATE '2010-01-01' AND month_start < DATE '2024-04-01'
        UNION ALL
        SELECT month_start, 'New this month', new_contributors FROM a.agg_monthly_active
        WHERE month_start >= DATE '2010-01-01' AND month_start < DATE '2024-04-01'
        ORDER BY 1, 2""",
    "retention_by_era": """
        SELECT CASE WHEN cohort_year <= 2014 THEN '2010-2014 cohorts' WHEN cohort_year <= 2019 THEN '2015-2019 cohorts'
                    ELSE '2020-2023 cohorts' END AS cohort_era,
               month_offset, sum(retained_users) AS retained_users, sum(cohort_size) AS eligible_users,
               round(sum(retained_users) / sum(cohort_size), 4) AS retention_rate
        FROM a.agg_retention_cohort WHERE cohort_year BETWEEN 2010 AND 2023 AND month_offset BETWEEN 1 AND 12
        GROUP BY 1, 2 ORDER BY 1, 2""",
    "question_funnel_yearly": """
        SELECT asked_year, asked, answered, accepted,
               round(answered / asked, 4) AS answered_rate, round(accepted / asked, 4) AS accepted_rate,
               median_hours_to_first_answer,
               CASE WHEN asked_year = 2024 THEN 'Partial year (Jan-Mar 2024)' ELSE 'Full year' END AS coverage
        FROM a.agg_question_funnel_yearly WHERE asked_year >= 2010 ORDER BY 1""",
    "tag_health": """
        SELECT tag, questions, answer_rate, accept_rate, median_hours_to_first_answer
        FROM a.agg_tag_health ORDER BY questions DESC LIMIT 15""",
    "experiment_aa": """
        WITH r AS (
          SELECT CASE metric WHEN 'activated_28d' THEN '1. Activated in 28d (primary)'
                             WHEN 'asked_28d' THEN '2. Asked in 28d'
                             WHEN 'answered_28d' THEN '3. Answered in 28d'
                             ELSE '4. Actions in 28d (guardrail)' END AS metric, *
          FROM a.agg_experiment_results)
        SELECT metric, '1. Control' AS stat,
               CASE WHEN metric_type = 'binary' THEN printf('%.2f%%', 100 * control_mean) ELSE printf('%.3f', control_mean) END AS value
        FROM r
        UNION ALL SELECT metric, '2. Treatment',
               CASE WHEN metric_type = 'binary' THEN printf('%.2f%%', 100 * treatment_mean) ELSE printf('%.3f', treatment_mean) END FROM r
        UNION ALL SELECT metric, '3. p-value', printf('%.2f', p_value) FROM r
        ORDER BY 1, 2""",
    "sla_status": """
        SELECT sla_type, dataset, description, observed, comparator || ' ' || CAST(threshold AS VARCHAR) AS target,
               CASE WHEN passed THEN 'Met' ELSE 'Breach' END AS status
        FROM a.sla_results WHERE checked_at = (SELECT max(checked_at) FROM a.sla_results)""",
    "kpis": """
        WITH m AS (SELECT * FROM a.fct_question_outcomes WHERE is_mature),
        r AS (SELECT sum(retained_users)::DOUBLE / sum(cohort_size) AS m1 FROM a.agg_retention_cohort WHERE month_offset = 1)
        SELECT * FROM (VALUES
          (1, 'accounts', format('{:,}', (SELECT count(*) FROM a.dim_user))),
          (2, 'contributors', format('{:,}', (SELECT count(DISTINCT user_key) FROM a.fct_user_events))),
          (3, 'user actions logged', format('{:,}', (SELECT count(*) FROM a.fct_user_events))),
          (4, 'month-1 contributor retention', printf('%.1f%%', 100 * (SELECT m1 FROM r))),
          (5, 'mature questions answered', printf('%.1f%%', 100.0 * (SELECT count(*) FROM m WHERE answers > 0) / (SELECT count(*) FROM m))),
          (6, 'median hours to first answer', printf('%.1f', (SELECT median(hours_to_first_answer) FROM m)))
        ) t(sort_order, metric, value)""",
}

DIM_S = ("string", "dimension", "nominal")
DIM_D = ("date", "dimension", "ordinal")
DIM_I = ("integer", "dimension", "ordinal")
MEA_I = ("integer", "measure", "quantitative")
MEA_R = ("real", "measure", "quantitative")
COLS = {
    "monthly_active_long": {"month_start": DIM_D, "segment": DIM_S, "contributors": MEA_I},
    "retention_by_era": {"cohort_era": DIM_S, "month_offset": DIM_I, "retained_users": MEA_I,
                         "eligible_users": MEA_I, "retention_rate": MEA_R},
    "question_funnel_yearly": {"asked_year": DIM_I, "asked": MEA_I, "answered": MEA_I, "accepted": MEA_I,
                               "answered_rate": MEA_R, "accepted_rate": MEA_R,
                               "median_hours_to_first_answer": MEA_R, "coverage": DIM_S},
    "tag_health": {"tag": DIM_S, "questions": MEA_I, "answer_rate": MEA_R, "accept_rate": MEA_R,
                   "median_hours_to_first_answer": MEA_R},
    "experiment_aa": {"metric": DIM_S, "stat": DIM_S, "value": DIM_S},
    "sla_status": {"sla_type": DIM_S, "dataset": DIM_S, "description": DIM_S, "observed": MEA_R,
                   "target": DIM_S, "status": DIM_S},
    "kpis": {"sort_order": MEA_I, "metric": DIM_S, "value": DIM_S},
}

CAPTIONS = {"month_start": "Month", "asked_year": "Year asked", "contributors": "Contributors",
            "segment": "Segment", "month_offset": "Months after first contribution",
            "retention_rate": "Retention", "cohort_era": "Cohort era", "asked": "Questions asked",
            "answered_rate": "Answered rate", "accept_rate": "Accept rate", "answer_rate": "Answer rate",
            "coverage": "Coverage", "tag": "Tag", "questions": "Questions", "sla_type": "SLA type",
            "description": "Check", "target": "Target", "status": "Status", "observed": "Observed",
            "metric": "Metric", "value": "Value", "accepted_rate": "Accepted rate",
            "control_mean": "Control", "treatment_mean": "Treatment", "delta": "Delta",
            "ci_low": "95% CI low", "ci_high": "95% CI high", "p_value": "p-value", "verdict": "Verdict", "stat": "Group"}
PERCENT = {"retention_rate", "answered_rate", "accept_rate", "answer_rate", "accepted_rate"}

LAKE_TABLES = {"agg_monthly_active": "analytics", "agg_retention_cohort": "analytics",
               "agg_question_funnel_yearly": "analytics", "agg_tag_health": "analytics",
               "agg_experiment_results": "analytics", "fct_question_outcomes": "analytics",
               "dim_user": "analytics", "fct_user_events": "analytics", "sla_results": "ops"}


def export():
    OUT.mkdir(exist_ok=True)
    for f in OUT.glob("*.csv"):
        f.unlink()
    con = duckdb.connect()
    con.execute("CREATE SCHEMA a")
    for t, layer in LAKE_TABLES.items():
        con.execute(f"CREATE VIEW a.{t} AS SELECT * FROM read_parquet('{LAKE / layer / t}/**/*.parquet', hive_partitioning = true)")
    for name, sql in EXTRACTS.items():
        rel = con.sql(sql)
        assert list(rel.columns) == list(COLS[name]), (name, rel.columns)
        rel.write_csv(str(OUT / f"{name}.csv"))
    con.close()


# ---------------------------------------------------------------- TWB
def ds_id(name):
    return f"federated.{name}"


DS_PALETTES = {}  # ds -> {field: {value: color}}


def datasource(name):
    cols = COLS[name]
    pal = DS_PALETTES.get(name, {})
    pal_xml = ""
    if pal:
        rules = "".join(
            f"<encoding attr='color' field='[none:{f}:nk]' type='palette'>"
            + "".join(f"<map to='{c}'><bucket>&quot;{v}&quot;</bucket></map>" for v, c in m.items())
            + "</encoding>" for f, m in pal.items())
        pal_xml = f"\n      <style><style-rule element='mark'>{rules}</style-rule></style>"
    rel_cols = "\n".join(
        f"            <column datatype='{dt}' name='{c}' ordinal='{i}' />"
        for i, (c, (dt, _, _)) in enumerate(cols.items()))
    def meta_col(c, dt, role, typ):
        cap = f" caption={quoteattr(CAPTIONS[c])}" if c in CAPTIONS else ""
        fmt = " default-format='p0%'" if c in PERCENT else ""
        return f"      <column{cap} datatype='{dt}'{fmt} name='[{c}]' role='{role}' type='{typ}' />"
    meta = "\n".join(meta_col(c, *v) for c, v in cols.items())
    return f"""    <datasource caption='{name}' inline='true' name='{ds_id(name)}' version='18.1'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='{name}' name='textscan.{name}'>
            <connection class='textscan' directory={quoteattr(WIN_DIR)} filename='{name}.csv' password='' server='' />
          </named-connection>
        </named-connections>
        <relation connection='textscan.{name}' name='{name}.csv' table='[{name}#csv]' type='table'>
          <columns character-set='UTF-8' header='yes' locale='en_US' separator=','>
{rel_cols}
          </columns>
        </relation>
      </connection>
{meta}{pal_xml}
    </datasource>"""


def inst(field, deriv, kind):
    """column-instance name like [sum:asked:qk]."""
    prefix = {"None": "none", "Sum": "sum", "Avg": "avg", "Attribute": "attr"}[deriv]
    return f"[{prefix}:{field}:{kind}]"


def worksheet(title, ds, deps, rows, cols, mark, encodings="", sort=None, filters="",
              palette=None, cell_size=None, subtitle=""):
    """deps: list of (field, derivation, kind) tuples used by the sheet."""
    c = COLS[ds]
    dep_cols = "\n".join(
        f"            <column datatype='{c[f][0]}' name='[{f}]' role='{c[f][1]}' type='{c[f][2]}' />"
        for f in dict.fromkeys(f for f, _, _ in deps))
    typ = {"nk": "nominal", "ok": "ordinal", "qk": "quantitative"}
    dep_inst = "\n".join(
        f"            <column-instance column='[{f}]' derivation='{d}' name='{inst(f, d, k)}' pivot='key' type='{typ[k]}' />"
        for f, d, k in deps)
    D = ds_id(ds)
    ref = lambda f, d, k: f"[{D}].{inst(f, d, k)}"
    enc = "".join(f"<{a} column='{ref(*x)}' />" for a, x in encodings) if encodings else ""
    sort_xml = ""
    if sort:
        dim, measure, direction = sort
        lvl = inst(*dim)
        sort_xml = (f"          <filter class='categorical' column='{ref(*dim)}'><groupfilter function='level-members' level='{lvl}' user:ui-enumeration='all' user:ui-marker='enumerate' /></filter>\n"
                    f"          <computed-sort column='{ref(*dim)}' direction='{direction}' using='{ref(*measure)}' />\n")
    rules = []
    if palette:
        field, mapping = palette
        pass  # Tableau 2026.2 ignored hand-written color maps; subtitles describe the default colors instead
    if cell_size:
        rules.append(f"<style-rule element='cell'><format attr='font-size' value='{cell_size}' /><format attr='font-weight' value='bold' /></style-rule>")
    style = f"<style>{''.join(rules)}</style>" if rules else "<style />"
    sub = f"<run fontsize='9' fontcolor='#6b7280'>&#10;{subtitle}</run>" if subtitle else ""
    return f"""    <worksheet name={quoteattr(title)}>
      <layout-options><title><formatted-text><run bold='true'>{title}</run>{sub}</formatted-text></title></layout-options>
      <table>
        <view>
          <datasources><datasource caption='{ds}' name='{D}' /></datasources>
          <datasource-dependencies datasource='{D}'>
{dep_cols}
{dep_inst}
          </datasource-dependencies>
{filters}{sort_xml}          <aggregation value='true' />
        </view>
        {style}
        <panes>
          <pane selection-relaxation-option='selection-relaxation-allow'>
            <view><breakdown value='auto' /></view>
            <mark class='{mark}' />
            {"<encodings>" + enc + "</encodings>" if enc else ""}
          </pane>
        </panes>
        <rows>{' / '.join(ref(*r) for r in rows)}</rows>
        <cols>{' / '.join(ref(*x) for x in cols)}</cols>
      </table>
      <simple-id uuid='{{{str(uuid.uuid4()).upper()}}}' />
    </worksheet>"""


SHEETS = []


def sheet(*a, **k):
    SHEETS.append((a[0], worksheet(*a, **k)))


BLUE, AMBER, GREY, GREEN, RED = "#2f5d8a", "#c98a2b", "#b8c2cf", "#2e7d4f", "#b4433a"


def build_sheets():
    sheet("Monthly Active Contributors", "monthly_active_long",
          [("month_start", "None", "qk"), ("contributors", "Sum", "qk"), ("segment", "None", "nk")],
          rows=[("contributors", "Sum", "qk")], cols=[("month_start", "None", "qk")], mark="Area",
          encodings=[("color", ("segment", "None", "nk"))],
          subtitle="Blue = first contribution that month, orange = returning. Peak 2017, declining since.")
    sheet("Retention by Cohort Era", "retention_by_era",
          [("month_offset", "None", "ok"), ("retention_rate", "Sum", "qk"), ("cohort_era", "None", "nk")],
          rows=[("retention_rate", "Sum", "qk")], cols=[("month_offset", "None", "ok")], mark="Line",
          encodings=[("color", ("cohort_era", "None", "nk"))],
          subtitle="Share of each first-contribution cohort active again N months later. Blue 2010-14, orange 2015-19, red 2020-23.")
    sheet("Questions Asked per Year", "question_funnel_yearly",
          [("asked_year", "None", "ok"), ("asked", "Sum", "qk"), ("coverage", "None", "nk")],
          rows=[("asked", "Sum", "qk")], cols=[("asked_year", "None", "ok")], mark="Bar",
          encodings=[("color", ("coverage", "None", "nk"))],
          subtitle="Orange = 2024, partial year (dump ends 2024-03-31).")
    sheet("Answered Rate per Year", "question_funnel_yearly",
          [("asked_year", "None", "ok"), ("answered_rate", "Sum", "qk"), ("accepted_rate", "Sum", "qk")],
          rows=[("answered_rate", "Sum", "qk")], cols=[("asked_year", "None", "ok")], mark="Line",
          encodings=[("text", ("answered_rate", "Sum", "qk"))],
          subtitle="Share of questions with at least one answer, by year asked.")
    sheet("Top Tags: Accept Rate", "tag_health",
          [("tag", "None", "nk"), ("accept_rate", "Sum", "qk"), ("questions", "Sum", "qk")],
          rows=[("tag", "None", "nk")], cols=[("accept_rate", "Sum", "qk")], mark="Bar",
          encodings=[("text", ("accept_rate", "Sum", "qk")), ("tooltip", ("questions", "Sum", "qk"))],
          sort=(("tag", "None", "nk"), ("accept_rate", "Sum", "qk"), "DESC"),
          subtitle="15 highest-volume tags, mature questions only.")
    sheet("A/A Experiment Check", "experiment_aa",
          [("metric", "None", "nk"), ("stat", "None", "nk"), ("value", "None", "nk")],
          rows=[("metric", "None", "nk")], cols=[("stat", "None", "nk")], mark="Text",
          encodings=[("text", ("value", "None", "nk"))],
          subtitle="24,587 real 2023 sign-ups, hash-split 50/50, no treatment. No metric differs.")
    sheet("Dataset SLAs", "sla_status",
          [("sla_type", "None", "nk"), ("description", "None", "nk"), ("target", "None", "nk"),
           ("status", "None", "nk"), ("observed", "Sum", "qk")],
          rows=[("sla_type", "None", "nk"), ("description", "None", "nk"), ("target", "None", "nk"),
                ("status", "None", "nk")], cols=[], mark="Text",
          encodings=[("text", ("observed", "Sum", "qk"))],
          subtitle="Checked on every pipeline run; any breach fails the run.")


def dashboard():
    # 1400 x 1100 fixed. Rows: KPIs | activity + retention | asked + answered | tags + A/A + SLAs
    W, H = 1400, 1100
    # Zone geometry is in Tableau's 0-100000 relative units, not pixels.
    sx = lambda px: round(px * 100000 / W)
    sy = lambda px: round(px * 100000 / H)
    zid = iter(range(10, 100))
    z = []
    z.append(f"<zone h='{sy(50)}' id='{next(zid)}' type-v2='title' w='100000' x='0' y='0' />")
    layout = [
        (150, 300, [("Monthly Active Contributors", 700), ("Retention by Cohort Era", 700)]),
        (450, 260, [("Questions Asked per Year", 700), ("Answered Rate per Year", 700)]),
        (710, 390, [("Top Tags: Accept Rate", 420), ("A/A Experiment Check", 480), ("Dataset SLAs", 500)]),
    ]
    for y, h, cells in layout:
        x = 0
        for name, w in cells:
            z.append(f"<zone h='{sy(h)}' id='{next(zid)}' name={quoteattr(name)} type-v2='visual' w='{sx(w)}' x='{sx(x)}' y='{sy(y)}' />")
            x += w
    import csv
    rows = list(csv.DictReader(open(OUT / "kpis.csv")))
    runs = "".join(
        f"<run bold='true' fontcolor='#1c2230' fontsize='18'>{r['value']}</run>"
        f"<run fontcolor='#5d6678' fontsize='11'> {r['metric']}{'    |    ' if i < len(rows) - 1 else ''}</run>"
        for i, r in enumerate(rows))
    note = ("<run fontcolor='#5d6678' fontsize='10'>&#10;Cross Validated (stats.stackexchange.com) public data dump, "
            "Feb 2009 - Mar 31, 2024. Built with PySpark. Mature questions = asked 14+ days before snapshot end.</run>")
    z.insert(1, f"<zone h='{sy(100)}' id='{next(zid)}' type-v2='text' w='100000' x='0' y='{sy(50)}'>"
                f"<formatted-text>{runs}{note}</formatted-text></zone>")
    zones = "\n          ".join(z)
    return f"""    <dashboard name='Q&amp;A Community Health'>
      <size maxheight='{H}' maxwidth='{W}' minheight='{H}' minwidth='{W}' sizing-mode='fixed' />
      <datasources />
      <zones>
        <zone h='100000' id='2' type-v2='layout-basic' w='100000' x='0' y='0'>
          {zones}
        </zone>
      </zones>
      <simple-id uuid='{{{str(uuid.uuid4()).upper()}}}' />
    </dashboard>"""


def windows():
    out = []
    for name, _ in SHEETS:
        out.append(f"    <window class='worksheet' hidden='true' name={quoteattr(name)}><cards><edge name='left'><strip size='160'><card type='pages' /><card type='filters' /><card type='marks' /></strip></edge><edge name='top'><strip size='31'><card type='columns' /></strip><strip size='31'><card type='rows' /></strip><strip size='31'><card type='title' /></strip></edge></cards><simple-id uuid='{{{str(uuid.uuid4()).upper()}}}' /></window>")
    vps = "".join(f"<viewpoint name={quoteattr(n)} />" for n, _ in SHEETS)
    out.append(f"    <window class='dashboard' maximized='true' name='Q&amp;A Community Health'><viewpoints>{vps}</viewpoints><active id='-1' /><simple-id uuid='{{{str(uuid.uuid4()).upper()}}}' /></window>")
    return "\n".join(out)


def build_twb():
    build_sheets()
    dss = "\n".join(datasource(n) for n in COLS)
    sheets = "\n".join(x for _, x in SHEETS)
    twb = f"""<?xml version='1.0' encoding='utf-8' ?>
<workbook original-version='18.1' source-build='2026.2.0 (20262.26.0528.2205)' source-platform='win' version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <document-format-change-manifest>
    <SheetIdentifierTracking />
    <SortTagCleanup />
    <WindowsPersistSimpleIdentifiers />
  </document-format-change-manifest>
  <preferences />
  <datasources>
{dss}
  </datasources>
  <worksheets>
{sheets}
  </worksheets>
  <dashboards>
{dashboard()}
  </dashboards>
  <windows>
{windows()}
  </windows>
</workbook>
"""
    (OUT / "Community_Product_Analytics.twb").write_text(twb, encoding="utf-8")


if __name__ == "__main__":
    export()
    build_twb()
    print("wrote", sorted(p.name for p in OUT.iterdir()))
