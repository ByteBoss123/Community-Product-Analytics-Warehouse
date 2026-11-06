-- Stage 3: dimensional model + product metric marts.
-- Access tiers:
--   restricted.*  : raw profile PII (display name, location, website, about-me). Owner team only.
--   analytics.*   : pseudonymous user_key only; safe for analysts, PMs, dashboards.
CREATE SCHEMA IF NOT EXISTS restricted;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Pseudonymous key. In production the salt would live in a secrets manager.
CREATE OR REPLACE MACRO ukey(id) AS
    CASE WHEN id IS NULL THEN NULL ELSE substr(sha256('cpa-salt-v1:' || id::VARCHAR), 1, 16) END;

CREATE OR REPLACE TABLE restricted.user_profile AS
SELECT ukey(user_id) AS user_key, user_id, display_name, location, website_url, about_me
FROM stg.users;

-- ---------------------------------------------------------------- dims
CREATE OR REPLACE TABLE analytics.dim_user AS
SELECT ukey(user_id) AS user_key, signed_up_at, signed_up_at::DATE AS signup_date,
       date_trunc('week', signed_up_at)::DATE AS signup_week, reputation
FROM stg.users;

CREATE OR REPLACE TABLE analytics.dim_date AS
SELECT d::DATE AS date_day, date_trunc('week', d)::DATE AS week_start,
       dayname(d) AS day_name, isodow(d) >= 6 AS is_weekend
FROM range(DATE '2016-08-01', (SELECT snapshot_end::DATE + 1 FROM stg.snapshot), INTERVAL 1 DAY) t(d);

CREATE OR REPLACE TABLE analytics.dim_post AS
SELECT post_id, post_type, parent_post_id, ukey(owner_user_id) AS owner_user_key,
       created_at, score, view_count, closed_at IS NOT NULL AS is_closed
FROM stg.posts;

CREATE OR REPLACE TABLE analytics.bridge_question_tag AS SELECT * FROM stg.post_tags;

-- ---------------------------------------------------------------- fact: unified event log
-- One row per attributable user action. Grain: (event_id).
-- Vote events other than favorite/bounty carry no voter id in the source, so
-- upvotes/downvotes are counted in post-level facts, never as user activity.
-- The accept actor is derived: only the question owner can accept an answer.
CREATE OR REPLACE TABLE analytics.fct_user_events AS
WITH ev AS (
    SELECT 'post:' || post_id AS event_id, owner_user_id AS user_id,
           post_type || '_posted' AS event_name, created_at AS event_ts, post_id
    FROM stg.posts WHERE post_type IN ('question', 'answer') AND owner_user_id IS NOT NULL
    UNION ALL
    SELECT 'comment:' || comment_id, user_id, 'comment_posted', created_at, post_id
    FROM stg.comments WHERE user_id IS NOT NULL
    UNION ALL
    SELECT 'edit:' || history_id, user_id, 'post_edited', created_at, post_id
    FROM stg.post_edits WHERE user_id IS NOT NULL
    UNION ALL
    SELECT 'vote:' || v.vote_id, q.owner_user_id, 'answer_accepted',
           v.vote_date::TIMESTAMP, v.post_id
    FROM stg.votes v
    JOIN stg.posts a ON a.post_id = v.post_id AND a.post_type = 'answer'
    JOIN stg.posts q ON q.post_id = a.parent_post_id
    WHERE v.vote_type = 'accept' AND q.owner_user_id IS NOT NULL
    UNION ALL
    SELECT 'vote:' || vote_id, user_id, vote_type, vote_date::TIMESTAMP, post_id
    FROM stg.votes WHERE vote_type IN ('favorite', 'bounty_start') AND user_id IS NOT NULL
)
SELECT event_id, ukey(user_id) AS user_key, event_name, event_ts,
       event_ts::DATE AS event_date, date_trunc('week', event_ts)::DATE AS event_week, post_id,
       -- day-grain timestamps (votes) are flagged so time-of-day analysis can exclude them
       event_name IN ('answer_accepted', 'favorite', 'bounty_start') AS is_day_grain
FROM ev;

-- ---------------------------------------------------------------- marts
-- Daily active contributors (DAC): users with >=1 attributable action that day.
CREATE OR REPLACE TABLE analytics.agg_daily_active AS
SELECT d.date_day, d.week_start, count(DISTINCT e.user_key) AS active_contributors,
       count(e.event_id) AS events
FROM analytics.dim_date d LEFT JOIN analytics.fct_user_events e ON e.event_date = d.date_day
GROUP BY ALL ORDER BY 1;

-- Weekly active contributors, split new (first-ever action that week) vs returning.
CREATE OR REPLACE TABLE analytics.agg_weekly_active AS
WITH first_act AS (
    SELECT user_key, min(event_week) AS first_week FROM analytics.fct_user_events GROUP BY 1
), wk AS (
    SELECT DISTINCT user_key, event_week FROM analytics.fct_user_events
)
SELECT w.event_week AS week_start, count(*) AS wac,
       count(*) FILTER (WHERE w.event_week = f.first_week) AS new_contributors,
       count(*) FILTER (WHERE w.event_week > f.first_week) AS returning_contributors
FROM wk w JOIN first_act f USING (user_key)
GROUP BY 1 ORDER BY 1;

-- Weekly retention by cohort (week of first contribution).
-- Full cohort x offset grid: offsets with zero retained users are emitted as 0, not
-- omitted (omitting them inflates pooled retention). Offsets are only generated up to
-- the last fully observed week; the final partial week is excluded.
CREATE OR REPLACE TABLE analytics.agg_retention_cohort AS
WITH first_act AS (
    SELECT user_key, min(event_week) AS cohort_week FROM analytics.fct_user_events GROUP BY 1
), act AS (
    SELECT DISTINCT user_key, event_week FROM analytics.fct_user_events
), sizes AS (
    SELECT cohort_week, count(*) AS cohort_size FROM first_act GROUP BY 1
), last_full_week AS (
    -- snapshot ends mid-week, so the last complete week starts 7 days before its week
    SELECT date_trunc('week', snapshot_end)::DATE - 7 AS w FROM stg.snapshot
), grid AS (
    SELECT s.cohort_week, s.cohort_size, k.week_offset
    FROM sizes s, last_full_week l,
         (SELECT range::INT AS week_offset FROM range(0, 30)) k
    WHERE s.cohort_week + 7 * k.week_offset <= l.w
), hits AS (
    SELECT f.cohort_week, ((a.event_week - f.cohort_week) / 7)::INT AS week_offset,
           count(DISTINCT a.user_key) AS retained_users
    FROM first_act f JOIN act a USING (user_key)
    GROUP BY 1, 2
)
SELECT g.cohort_week, g.cohort_size, g.week_offset,
       coalesce(h.retained_users, 0) AS retained_users,
       round(coalesce(h.retained_users, 0) / g.cohort_size, 4) AS retention_rate
FROM grid g LEFT JOIN hits h USING (cohort_week, week_offset)
ORDER BY 1, 3;

-- Signup -> first contribution funnel (users who signed up inside the snapshot).
CREATE OR REPLACE TABLE analytics.agg_activation AS
WITH first_act AS (
    SELECT user_key, min(event_ts) AS first_event_ts FROM analytics.fct_user_events GROUP BY 1
)
SELECT u.signup_week,
       count(*) AS signups,
       count(f.user_key) AS ever_contributed,
       count(*) FILTER (WHERE f.first_event_ts <= u.signed_up_at + INTERVAL 7 DAY) AS contributed_within_7d,
       -- users who signed up in the last 7 days of the snapshot have not had a full window
       max(s.snapshot_end) - INTERVAL 7 DAY >= max(u.signed_up_at) AS window_complete
FROM analytics.dim_user u LEFT JOIN first_act f USING (user_key) CROSS JOIN stg.snapshot s
GROUP BY 1 ORDER BY 1;

-- Question funnel: asked -> answered -> accepted, with time to first answer.
-- Right-censoring: only questions asked >= 14 days before snapshot end count as "mature".
CREATE OR REPLACE TABLE analytics.fct_question_outcomes AS
WITH q AS (SELECT * FROM stg.posts WHERE post_type = 'question'),
a AS (
    SELECT parent_post_id AS question_id, min(created_at) AS first_answer_at, count(*) AS answers,
           max(score) AS top_answer_score
    FROM stg.posts WHERE post_type = 'answer' GROUP BY 1
)
SELECT q.post_id AS question_id, q.created_at, date_trunc('week', q.created_at)::DATE AS asked_week,
       coalesce(a.answers, 0) AS answers, a.first_answer_at,
       date_diff('minute', q.created_at, a.first_answer_at) / 60.0 AS hours_to_first_answer,
       q.accepted_answer_id IS NOT NULL AS has_accepted_answer,
       q.closed_at IS NOT NULL AS is_closed, q.score, q.view_count,
       q.created_at <= (SELECT snapshot_end FROM stg.snapshot) - INTERVAL 14 DAY AS is_mature
FROM q LEFT JOIN a ON a.question_id = q.post_id;

CREATE OR REPLACE TABLE analytics.agg_question_funnel_weekly AS
SELECT asked_week, count(*) AS asked,
       count(*) FILTER (WHERE answers > 0) AS answered,
       count(*) FILTER (WHERE has_accepted_answer) AS accepted,
       round(median(hours_to_first_answer), 2) AS median_hours_to_first_answer,
       bool_and(is_mature) AS all_mature
FROM analytics.fct_question_outcomes GROUP BY 1 ORDER BY 1;

-- Tag health: answer and acceptance rate by tag (mature questions only).
CREATE OR REPLACE TABLE analytics.agg_tag_health AS
SELECT t.tag, count(*) AS questions,
       round(avg((o.answers > 0)::INT), 4) AS answer_rate,
       round(avg(o.has_accepted_answer::INT), 4) AS accept_rate,
       round(median(o.hours_to_first_answer), 2) AS median_hours_to_first_answer
FROM analytics.bridge_question_tag t JOIN analytics.fct_question_outcomes o ON o.question_id = t.post_id
WHERE o.is_mature
GROUP BY 1 ORDER BY questions DESC;

-- Post-level engagement facts (anonymous votes live here, not in user activity).
CREATE OR REPLACE TABLE analytics.fct_post_votes_daily AS
SELECT post_id, vote_date, vote_type, count(*) AS votes
FROM stg.votes GROUP BY ALL;
