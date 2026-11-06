# Logging Spec and Data Contract: Community Q&A Product

Scope: the contribution loop of a community Q&A product (ask, answer, comment, edit, accept, favorite).
Source: public Stack Exchange data dump for Artificial Intelligence Stack Exchange, 2016-08-02 to
2016-12-11 (site launch through ~19 weeks), CC BY-SA. Mirror: `bigdata-vandy/stack-ex`
@ `a9ad674`, files checksummed in `data/SHA256SUMS`.

## 1. Business questions this logging must answer

| # | Question | Metric | Table |
|---|----------|--------|-------|
| Q1 | Are new members becoming contributors? | signup to first contribution within 7 days | `analytics.agg_activation` |
| Q2 | Do contributors come back? | weekly cohort retention | `analytics.agg_retention_cohort` |
| Q3 | Does an asked question get resolved? | answered rate, accepted rate, hours to first answer | `analytics.fct_question_outcomes` |
| Q4 | Which topics are underserved? | answer/accept rate by tag | `analytics.agg_tag_health` |
| Q5 | How big is the active community? | daily/weekly active contributors | `analytics.agg_daily_active`, `agg_weekly_active` |

## 2. Event taxonomy (`analytics.fct_user_events`, grain = one user action)

| event_name | Source | Actor field | Timestamp grain | Notes |
|---|---|---|---|---|
| question_posted | Posts (PostTypeId=1) | OwnerUserId | millisecond | |
| answer_posted | Posts (PostTypeId=2) | OwnerUserId | millisecond | |
| comment_posted | Comments | UserId | millisecond | |
| post_edited | PostHistory (types 4,5,6) | UserId | millisecond | initial-creation rows (1,2,3) excluded to avoid double counting posts |
| answer_accepted | Votes (VoteTypeId=1) | **derived**: owner of parent question | day | only the asker can accept, so actor is inferred |
| favorite | Votes (VoteTypeId=5) | UserId | day | |
| bounty_start | Votes (VoteTypeId=8) | UserId | day | |

Required fields on every event: `event_id` (unique, source-prefixed), `user_key`, `event_name`,
`event_ts`, `event_date`, `event_week`, `post_id`, `is_day_grain`.

## 3. Logging gaps found in the source (and the fix I would request)

1. **Votes are anonymous.** 271 of 5,323 votes (5.1%) carry a voter id (favorites and bounties only).
   Upvotes and downvotes, the most frequent engagement action, cannot be attributed to a user, so
   "active contributors" undercounts people who only vote.
   *Fix:* log a hashed `voter_key` on every vote event into a restricted table, expose only
   aggregate counts downstream.
2. **No read/view events.** Only a lifetime `ViewCount` per post exists, so true DAU (including lurkers)
   and question-impression funnels cannot be built. Metrics here are explicitly *contributor* actives.
   *Fix:* a sampled `post_viewed` event with `user_key` (nullable for logged-out) and `session_id`.
3. **Vote timestamps are truncated to the day.** Accept/favorite events cannot be used for
   time-to-accept in hours. Flagged by `is_day_grain`.
4. **Deleted posts are absent but their votes are not.** 394 votes (7.4%) point at post ids missing from
   the dump. They are quarantined in `quarantine.votes_orphan_post`, not dropped.
   *Fix:* emit a `post_deleted` tombstone event so downstream joins can resolve instead of orphaning.

## 4. Privacy classification

| Field | Class | Where it lives |
|---|---|---|
| DisplayName, Location, WebsiteUrl, AboutMe | PII / free-text profile | `restricted.user_profile` only |
| Raw user Id | direct identifier | `restricted.user_profile` only |
| user_key (salted SHA-256, 16 hex) | pseudonymous | `analytics.*` |
| post content (Body, Title) | user-generated content | not loaded into analytics marts |

Enforced by SLA check `privacy`: zero PII or raw-id columns in the `analytics` schema
(test `test_pii_column_breaches_privacy` proves the check fires).

## 5. Dataset SLAs (owner: product-analytics data engineering)

| Dataset | SLA | Threshold |
|---|---|---|
| fct_user_events | freshness: latest event date vs source snapshot end | <= 1 day |
| stg.votes | completeness: raw = staged + quarantined | difference = 0 |
| fct_user_events | completeness: every attributable post and comment present | 0 missing |
| fct_user_events, dim_user | uniqueness of grain | 0 duplicates |
| fct_user_events | referential integrity to dim_user | 0 orphans |
| fct_question_outcomes | validity: no negative time-to-answer | 0 rows |
| agg_retention_cohort | completeness of cohort grid; rates in [0,1]; week 0 = 1.0 | 0 violations |
| analytics.* | privacy: no PII / raw-id columns | 0 columns |
| pipeline run | end-to-end run time (Airflow `dagrun_timeout`) | 2 hours |

Every check writes a row to `ops.sla_results`; any breach fails the Airflow `sla_checks` task.
