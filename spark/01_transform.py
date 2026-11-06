"""Spark SQL transforms: raw Parquet -> staged -> analytics marts (+ restricted PII, quarantine).

Layout (lake/):
  stg/<table>/                      typed, cleaned, community account (-1) removed
  quarantine/votes_orphan_post/     votes whose post is absent from the dump (deleted posts)
  restricted/user_profile/          PII: display name, location, website. Owner team only.
  analytics/fct_user_events/        partitioned by event_year
  analytics/<dims and marts>/
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession

LAKE = Path(__file__).resolve().parents[1] / "lake"
SALT = "cpa-salt-v1:"  # production: read from a secrets manager


def spark_session():
    return (SparkSession.builder.appName("community-product-analytics")
            .master("local[*]").config("spark.sql.shuffle.partitions", "16")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "4g").getOrCreate())


def view(spark, layer, name):
    spark.read.parquet(str(LAKE / layer / (name if layer != "raw" else f"{name}.parquet"))).createOrReplaceTempView(
        f"{layer}_{name}")


def save(spark, sql, layer, name, partition_by=None):
    df = spark.sql(sql)
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.parquet(str(LAKE / layer / name))
    view(spark, layer, name)
    return df


def main():
    spark = spark_session()
    spark.udf.register("noop", lambda x: x)
    for t in ["posts", "comments", "votes", "posthistory", "users", "badges", "tags", "postlinks"]:
        view(spark, "raw", t)
    ukey = lambda col: f"CASE WHEN {col} IS NULL THEN NULL ELSE substr(sha2(concat('{SALT}', CAST({col} AS STRING)), 256), 1, 16) END"

    # ------------------------------------------------------------ staging
    save(spark, """
        SELECT max(ts) AS snapshot_end FROM (
          SELECT max(CreationDate) ts FROM raw_posts UNION ALL
          SELECT max(CreationDate) FROM raw_comments UNION ALL
          SELECT max(CreationDate) FROM raw_posthistory UNION ALL
          SELECT max(CreationDate) FROM raw_users)""", "stg", "snapshot")

    save(spark, """
        SELECT Id AS post_id,
               CASE PostTypeId WHEN 1 THEN 'question' WHEN 2 THEN 'answer' WHEN 4 THEN 'tag_wiki_excerpt'
                               WHEN 5 THEN 'tag_wiki' ELSE 'other' END AS post_type,
               ParentId AS parent_post_id, AcceptedAnswerId AS accepted_answer_id,
               CreationDate AS created_at, Score AS score, ViewCount AS view_count,
               NULLIF(OwnerUserId, -1) AS owner_user_id, Tags AS tags_raw,
               AnswerCount AS answer_count, CommentCount AS comment_count, ClosedDate AS closed_at
        FROM raw_posts""", "stg", "posts")

    save(spark, """
        SELECT v.*, 'post_id not present in posts (deleted post)' AS reason
        FROM raw_votes v LEFT ANTI JOIN raw_posts p ON v.PostId = p.Id""", "quarantine", "votes_orphan_post")

    save(spark, """
        SELECT v.Id AS vote_id, v.PostId AS post_id, v.VoteTypeId AS vote_type_id,
               CASE v.VoteTypeId WHEN 1 THEN 'accept' WHEN 2 THEN 'upvote' WHEN 3 THEN 'downvote'
                    WHEN 5 THEN 'favorite' WHEN 8 THEN 'bounty_start' WHEN 9 THEN 'bounty_close'
                    WHEN 10 THEN 'deletion' WHEN 11 THEN 'undeletion' WHEN 15 THEN 'moderator_review'
                    WHEN 16 THEN 'approve_edit' ELSE 'other' END AS vote_type,
               NULLIF(v.UserId, -1) AS user_id, CAST(v.CreationDate AS DATE) AS vote_date
        FROM raw_votes v LEFT SEMI JOIN raw_posts p ON v.PostId = p.Id""", "stg", "votes")

    save(spark, """
        SELECT Id AS comment_id, PostId AS post_id, Score AS score, CreationDate AS created_at,
               NULLIF(UserId, -1) AS user_id FROM raw_comments""", "stg", "comments")

    save(spark, """
        SELECT Id AS history_id, PostId AS post_id, CreationDate AS created_at,
               NULLIF(UserId, -1) AS user_id, PostHistoryTypeId AS history_type_id
        FROM raw_posthistory WHERE PostHistoryTypeId IN (4, 5, 6)""", "stg", "post_edits")

    save(spark, """
        SELECT Id AS user_id, CreationDate AS signed_up_at, Reputation AS reputation,
               LastAccessDate AS last_access_at, DisplayName AS display_name, Location AS location,
               WebsiteUrl AS website_url
        FROM raw_users WHERE Id <> -1""", "stg", "users")

    # Newer dumps store tags as |a|b|; older as <a><b>. Handle both.
    save(spark, """
        SELECT post_id, tag FROM (
          SELECT post_id, explode(split(regexp_replace(tags_raw, '^[|<]|[|>]$', ''), '\\\\||><')) AS tag
          FROM stg_posts WHERE post_type = 'question' AND tags_raw IS NOT NULL AND tags_raw <> '')
        WHERE tag <> ''""", "stg", "post_tags")

    # ------------------------------------------------------------ restricted + dims
    save(spark, f"""
        SELECT {ukey('user_id')} AS user_key, user_id, display_name, location, website_url
        FROM stg_users""", "restricted", "user_profile")

    save(spark, f"""
        SELECT {ukey('user_id')} AS user_key, signed_up_at, CAST(signed_up_at AS DATE) AS signup_date,
               CAST(date_trunc('MONTH', signed_up_at) AS DATE) AS signup_month, reputation
        FROM stg_users""", "analytics", "dim_user")

    save(spark, f"""
        SELECT post_id, post_type, parent_post_id, {ukey('owner_user_id')} AS owner_user_key,
               created_at, score, view_count, closed_at IS NOT NULL AS is_closed
        FROM stg_posts""", "analytics", "dim_post")

    save(spark, "SELECT * FROM stg_post_tags", "analytics", "bridge_question_tag")

    # ------------------------------------------------------------ event fact
    save(spark, f"""
        WITH ev AS (
          SELECT concat('post:', post_id) AS event_id, owner_user_id AS user_id,
                 concat(post_type, '_posted') AS event_name, created_at AS event_ts, post_id
          FROM stg_posts WHERE post_type IN ('question', 'answer') AND owner_user_id IS NOT NULL
          UNION ALL
          SELECT concat('comment:', comment_id), user_id, 'comment_posted', created_at, post_id
          FROM stg_comments WHERE user_id IS NOT NULL
          UNION ALL
          SELECT concat('edit:', history_id), user_id, 'post_edited', created_at, post_id
          FROM stg_post_edits WHERE user_id IS NOT NULL
          UNION ALL
          SELECT concat('vote:', v.vote_id), q.owner_user_id, 'answer_accepted',
                 CAST(v.vote_date AS TIMESTAMP), v.post_id
          FROM stg_votes v
          JOIN stg_posts a ON a.post_id = v.post_id AND a.post_type = 'answer'
          JOIN stg_posts q ON q.post_id = a.parent_post_id
          WHERE v.vote_type = 'accept' AND q.owner_user_id IS NOT NULL
          UNION ALL
          SELECT concat('vote:', vote_id), user_id, vote_type, CAST(vote_date AS TIMESTAMP), post_id
          FROM stg_votes WHERE vote_type IN ('favorite', 'bounty_start') AND user_id IS NOT NULL
        )
        SELECT event_id, {ukey('user_id')} AS user_key, event_name, event_ts,
               CAST(event_ts AS DATE) AS event_date,
               CAST(date_trunc('WEEK', event_ts) AS DATE) AS event_week,
               CAST(date_trunc('MONTH', event_ts) AS DATE) AS event_month,
               post_id, event_name IN ('answer_accepted', 'favorite', 'bounty_start') AS is_day_grain,
               year(event_ts) AS event_year
        FROM ev""", "analytics", "fct_user_events", partition_by="event_year")

    # ------------------------------------------------------------ marts
    save(spark, """
        WITH first_act AS (SELECT user_key, min(event_month) AS first_month FROM analytics_fct_user_events GROUP BY 1),
        m AS (SELECT DISTINCT user_key, event_month FROM analytics_fct_user_events)
        SELECT m.event_month AS month_start, count(*) AS mac,
               sum(CASE WHEN m.event_month = f.first_month THEN 1 ELSE 0 END) AS new_contributors,
               sum(CASE WHEN m.event_month > f.first_month THEN 1 ELSE 0 END) AS returning_contributors
        FROM m JOIN first_act f USING (user_key) GROUP BY 1 ORDER BY 1""", "analytics", "agg_monthly_active")

    save(spark, """
        WITH first_act AS (SELECT user_key, min(event_week) AS first_week FROM analytics_fct_user_events GROUP BY 1),
        w AS (SELECT DISTINCT user_key, event_week FROM analytics_fct_user_events)
        SELECT w.event_week AS week_start, count(*) AS wac,
               sum(CASE WHEN w.event_week = f.first_week THEN 1 ELSE 0 END) AS new_contributors,
               sum(CASE WHEN w.event_week > f.first_week THEN 1 ELSE 0 END) AS returning_contributors
        FROM w JOIN first_act f USING (user_key) GROUP BY 1 ORDER BY 1""", "analytics", "agg_weekly_active")

    # Monthly cohort retention, full cohort x offset grid (zero cells emitted, partial final month excluded).
    save(spark, """
        WITH first_act AS (SELECT user_key, min(event_month) AS cohort_month FROM analytics_fct_user_events GROUP BY 1),
        act AS (SELECT DISTINCT user_key, event_month FROM analytics_fct_user_events),
        sizes AS (SELECT cohort_month, count(*) AS cohort_size FROM first_act GROUP BY 1),
        last_full AS (SELECT add_months(CAST(date_trunc('MONTH', snapshot_end) AS DATE), -1) AS m FROM stg_snapshot),
        grid AS (
          SELECT s.cohort_month, s.cohort_size, k.week_offset AS month_offset
          FROM sizes s CROSS JOIN last_full l CROSS JOIN (SELECT CAST(id AS INT) AS week_offset FROM range(0, 13)) k
          WHERE add_months(s.cohort_month, k.week_offset) <= l.m),
        hits AS (
          SELECT f.cohort_month, CAST(months_between(a.event_month, f.cohort_month) AS INT) AS month_offset,
                 count(DISTINCT a.user_key) AS retained_users
          FROM first_act f JOIN act a USING (user_key) GROUP BY 1, 2)
        SELECT g.cohort_month, year(g.cohort_month) AS cohort_year, g.cohort_size, g.month_offset,
               coalesce(h.retained_users, 0) AS retained_users,
               round(coalesce(h.retained_users, 0) / g.cohort_size, 4) AS retention_rate
        FROM grid g LEFT JOIN hits h USING (cohort_month, month_offset)
        ORDER BY 1, 4""", "analytics", "agg_retention_cohort")

    save(spark, """
        WITH first_act AS (SELECT user_key, min(event_ts) AS first_event_ts FROM analytics_fct_user_events GROUP BY 1)
        SELECT u.signup_month, count(*) AS signups, count(f.user_key) AS ever_contributed,
               sum(CASE WHEN f.first_event_ts <= u.signed_up_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS contributed_within_7d,
               max(u.signed_up_at) <= max(s.snapshot_end) - INTERVAL 7 DAYS AS window_complete
        FROM analytics_dim_user u LEFT JOIN first_act f USING (user_key) CROSS JOIN stg_snapshot s
        GROUP BY 1 ORDER BY 1""", "analytics", "agg_activation")

    # Merged questions (PostHistoryTypeId 38 = merge destination) inherit answers written before the
    # destination question existed. Time-to-first-answer only counts answers posted after the question.
    save(spark, """
        WITH q AS (SELECT * FROM stg_posts WHERE post_type = 'question'),
        merged AS (SELECT DISTINCT PostId AS question_id FROM raw_posthistory WHERE PostHistoryTypeId = 38),
        a AS (SELECT ans.parent_post_id AS question_id, count(*) AS answers,
                     min(CASE WHEN ans.created_at >= qq.created_at THEN ans.created_at END) AS first_answer_at,
                     sum(CASE WHEN ans.created_at < qq.created_at THEN 1 ELSE 0 END) AS answers_predating_question
              FROM stg_posts ans JOIN stg_posts qq ON qq.post_id = ans.parent_post_id
              WHERE ans.post_type = 'answer' GROUP BY 1)
        SELECT q.post_id AS question_id, q.created_at,
               m.question_id IS NOT NULL AS is_merge_destination,
               coalesce(a.answers_predating_question, 0) AS answers_predating_question,
               CAST(date_trunc('MONTH', q.created_at) AS DATE) AS asked_month, year(q.created_at) AS asked_year,
               coalesce(a.answers, 0) AS answers, a.first_answer_at,
               (unix_timestamp(a.first_answer_at) - unix_timestamp(q.created_at)) / 3600.0 AS hours_to_first_answer,
               q.accepted_answer_id IS NOT NULL AS has_accepted_answer, q.closed_at IS NOT NULL AS is_closed,
               q.score, q.view_count,
               q.created_at <= (SELECT snapshot_end FROM stg_snapshot) - INTERVAL 14 DAYS AS is_mature
        FROM q LEFT JOIN a ON a.question_id = q.post_id LEFT JOIN merged m ON m.question_id = q.post_id""",
         "analytics", "fct_question_outcomes")

    save(spark, """
        SELECT asked_year, count(*) AS asked,
               sum(CASE WHEN answers > 0 THEN 1 ELSE 0 END) AS answered,
               sum(CASE WHEN has_accepted_answer THEN 1 ELSE 0 END) AS accepted,
               sum(CASE WHEN is_closed THEN 1 ELSE 0 END) AS closed,
               round(percentile_approx(hours_to_first_answer, 0.5, 10000), 2) AS median_hours_to_first_answer,
               bool_and(is_mature) AS all_mature
        FROM analytics_fct_question_outcomes GROUP BY 1 ORDER BY 1""", "analytics", "agg_question_funnel_yearly")

    save(spark, """
        SELECT t.tag, count(*) AS questions,
               round(avg(CASE WHEN o.answers > 0 THEN 1 ELSE 0 END), 4) AS answer_rate,
               round(avg(CASE WHEN o.has_accepted_answer THEN 1 ELSE 0 END), 4) AS accept_rate,
               round(percentile_approx(o.hours_to_first_answer, 0.5, 10000), 2) AS median_hours_to_first_answer
        FROM analytics_bridge_question_tag t JOIN analytics_fct_question_outcomes o ON o.question_id = t.post_id
        WHERE o.is_mature GROUP BY 1 ORDER BY questions DESC""", "analytics", "agg_tag_health")

    save(spark, "SELECT post_id, vote_date, vote_type, count(*) AS votes FROM stg_votes GROUP BY ALL",
         "analytics", "fct_post_votes_daily")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
