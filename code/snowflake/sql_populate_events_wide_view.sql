INSERT INTO
    GITHUB_EVENTS_WIDE (ID, PAYLOAD_CREATED_AT, ISSUE_CLOSED_AT, ISSUE_URL, MERGE_AT, LANGUAGE, RELEASE_PUBLISHED_AT, REVIEW_SUBMITTED_AT)
SELECT
    VARIANT_COL:id,
    -- unified payload-created timestamp
    CASE
        WHEN VARIANT_COL:type = 'IssuesEvent' THEN VARIANT_COL:payload:issue:created_at
        WHEN VARIANT_COL:type = 'ReleaseEvent' THEN VARIANT_COL:payload:release:created_at
        WHEN VARIANT_COL:type IN ('PullRequestEvent','PullRequestReviewEvent','PullRequestReviewCommentEvent')
            THEN VARIANT_COL:payload:pull_request:created_at
        ELSE NULL
    END AS payload_created_at,

    -- IssuesEvent details
    CASE
        WHEN VARIANT_COL:type = 'IssuesEvent' THEN VARIANT_COL:payload:issue:closed_at
        ELSE NULL
    END AS issue_closed_at,

    CASE
        WHEN VARIANT_COL:type = 'PullRequestEvent' THEN VARIANT_COL:payload:pull_request:issue_url
        ELSE NULL
    END AS issue_url,

    -- PullRequest merged timestamp (only for PR-related events)
    CASE
        WHEN VARIANT_COL:type IN ('PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent')
            THEN VARIANT_COL:payload:pull_request:merged_at
        ELSE NULL
    END AS merged_at,

    -- language for various event-types
    CASE
        WHEN VARIANT_COL:type = 'ForkEvent' THEN VARIANT_COL:payload:forkeraw:language
        WHEN VARIANT_COL:type = 'PullRequestEvent' THEN VARIANT_COL:payload:pull_request:base:repo:language
        WHEN VARIANT_COL:type IN ('PullRequestReviewEvent', 'PullRequestReviewCommentEvent')
            THEN VARIANT_COL:payload:pull_request:head:repo:language
        ELSE NULL
    END AS language,

    -- ReleaseEvent details
    CASE
        WHEN VARIANT_COL:type = 'ReleaseEvent' THEN VARIANT_COL:payload:release:published_at
        ELSE NULL
    END AS release_published_at,
    
    -- review submission time only for reviews
    CASE
        WHEN VARIANT_COL:type = 'PullRequestReviewEvent' THEN VARIANT_COL:payload:review:submitted_at
        ELSE NULL
    END AS review_submitted_at

FROM GITHUB_RAW_DATA;

