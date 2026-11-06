-- Stage 2: staging layer. Type cleanup, quarantine of rows that break referential
-- integrity, and removal of the system "Community" account (Id = -1) from user activity.
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- Snapshot end = latest timestamp anywhere in the dump. Used for right-censoring.
CREATE OR REPLACE TABLE stg.snapshot AS
SELECT max(ts) AS snapshot_end FROM (
    SELECT max(CreationDate) ts FROM raw.posts UNION ALL
    SELECT max(CreationDate) FROM raw.comments UNION ALL
    SELECT max(CreationDate) FROM raw.posthistory UNION ALL
    SELECT max(CreationDate) FROM raw.users
);

CREATE OR REPLACE TABLE stg.posts AS
SELECT Id AS post_id,
       CASE PostTypeId WHEN 1 THEN 'question' WHEN 2 THEN 'answer'
                       WHEN 4 THEN 'tag_wiki_excerpt' WHEN 5 THEN 'tag_wiki' ELSE 'other' END AS post_type,
       ParentId AS parent_post_id, AcceptedAnswerId AS accepted_answer_id,
       CreationDate AS created_at, Score AS score, ViewCount AS view_count,
       NULLIF(OwnerUserId, -1) AS owner_user_id, Tags AS tags_raw,
       AnswerCount AS answer_count, CommentCount AS comment_count, ClosedDate AS closed_at
FROM raw.posts;

-- Votes on posts that are absent from the dump (deleted posts are excluded from
-- public dumps) cannot be joined to a post. Quarantined, not silently dropped.
CREATE OR REPLACE TABLE quarantine.votes_orphan_post AS
SELECT v.*, 'post_id not present in posts (deleted post)' AS reason
FROM raw.votes v LEFT JOIN raw.posts p ON v.PostId = p.Id
WHERE p.Id IS NULL;

CREATE OR REPLACE TABLE stg.votes AS
SELECT v.Id AS vote_id, v.PostId AS post_id, v.VoteTypeId AS vote_type_id,
       CASE v.VoteTypeId WHEN 1 THEN 'accept' WHEN 2 THEN 'upvote' WHEN 3 THEN 'downvote'
            WHEN 5 THEN 'favorite' WHEN 8 THEN 'bounty_start' WHEN 9 THEN 'bounty_close'
            WHEN 10 THEN 'deletion' WHEN 11 THEN 'undeletion' WHEN 15 THEN 'moderator_review'
            WHEN 16 THEN 'approve_edit' ELSE 'other' END AS vote_type,
       NULLIF(v.UserId, -1) AS user_id, v.CreationDate::DATE AS vote_date
FROM raw.votes v JOIN raw.posts p ON v.PostId = p.Id;

CREATE OR REPLACE TABLE stg.comments AS
SELECT Id AS comment_id, PostId AS post_id, Score AS score, CreationDate AS created_at,
       NULLIF(UserId, -1) AS user_id
FROM raw.comments;

CREATE OR REPLACE TABLE stg.post_edits AS
SELECT Id AS history_id, PostId AS post_id, CreationDate AS created_at,
       NULLIF(UserId, -1) AS user_id, PostHistoryTypeId AS history_type_id
FROM raw.posthistory
WHERE PostHistoryTypeId IN (4, 5, 6);   -- title, body, tag edits (excludes initial 1/2/3 creation rows)

CREATE OR REPLACE TABLE stg.users AS
SELECT Id AS user_id, CreationDate AS signed_up_at, Reputation AS reputation,
       LastAccessDate AS last_access_at,
       -- PII / free-text profile fields stay in a restricted table (see 02_marts.sql)
       DisplayName AS display_name, Location AS location, WebsiteUrl AS website_url,
       AboutMe AS about_me
FROM raw.users WHERE Id <> -1;

CREATE OR REPLACE TABLE stg.post_tags AS
SELECT post_id, unnest(regexp_extract_all(tags_raw, '<([^>]+)>', 1)) AS tag
FROM stg.posts WHERE post_type = 'question' AND tags_raw IS NOT NULL;
