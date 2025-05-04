COPY INTO GITHUB_RAW_DATA -- Your destination table
FROM @my_gcs_stage -- Your external stage
PATTERN = '.*\.json' -- Example: Load only csv files
ON_ERROR = 'ABORT_STATEMENT'