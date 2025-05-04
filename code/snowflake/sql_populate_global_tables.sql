-- insert actor data from raw data
INSERT INTO
    ACTOR (ID, LOGIN, GRAVATAR_ID, URL, AVATAR_URL)
SELECT DISTINCT
    VARIANT_COL:actor:id AS actor_id,
    VARIANT_COL:actor:login AS login,
    VARIANT_COL:actor:gravatar_id AS gravatar_id,
    VARIANT_COL:actor:url AS actor_id,
    VARIANT_COL:actor:avatar_url AS avatar_url
FROM
    GITHUB_RAW_DATA;

-- insert repo data from raw data
INSERT INTO
    REPO (ID, NAME, URL)
SELECT DISTINCT
    VARIANT_COL:repo:id AS actor_id,
    VARIANT_COL:repo:name AS name,
    VARIANT_COL:repo:url AS url,
FROM
    GITHUB_RAW_DATA;

-- insert org data from raw data
INSERT INTO
    ORG (ID, LOGIN, GRAVATAR_ID, URL, AVATAR_URL)
SELECT DISTINCT
    VARIANT_COL:org:id AS actor_id,
    VARIANT_COL:org:login AS login,
    VARIANT_COL:org:gravatar_id AS gravatar_id,
    VARIANT_COL:org:url AS actor_id,
    VARIANT_COL:org:avatar_url AS avatar_url
FROM
    GITHUB_RAW_DATA
WHERE
    VARIANT_COL:org IS NOT NULL; -- neede becase not all the instances have org


-- insert global events data from raw data
-- NOTE: execute repo and actor insert first
INSERT INTO
    GLOBAL_EVENTS (ID, TYPE, ACTOR_ID, REPO_ID, ORG_ID, PUBLIC, CREATED_AT)
SELECT
    VARIANT_COL:id AS id,
    VARIANT_COL:type AS type,
    VARIANT_COL:actor:id AS actor_id,
    VARIANT_COL:repo:id AS repo_id,
    VARIANT_COL:org:id AS org_id,
    VARIANT_COL:public AS public,
    VARIANT_COL:created_at AS created_at,
FROM
    GITHUB_RAW_DATA;
