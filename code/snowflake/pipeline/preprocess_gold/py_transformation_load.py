# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_part, to_date, hour, dayofweek, dayname, date_trunc, datediff, when, to_timestamp, regexp_count, lower, concat

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.

    events_table = 'PUBLIC.GLOBAL_EVENTS'
    actor_table = 'PUBLIC.ACTOR'
    repo_table = 'PUBLIC.REPO'
    orgs_table = 'PUBLIC.ORG'
    payloads_table = 'PUBLIC.GITHUB_EVENTS_WIDE'

    events_df = session.table(events_table)
    actor_df = session.table(actor_table)
    repo_df = session.table(repo_table)
    orgs_df = session.table(orgs_table)
    payloads_df = session.table(payloads_table)
    
    # Print a sample of the dataframe to standard output.
    events_df.show()

    def transform_data(events, actors, repo, orgs, payloads):
        """
        Transforms and enriches GitHub event data by joining multiple tables, 
        casting necessary columns, calculating derived metrics, and performing aggregation.
    
        Args:
            events (DataFrame): DataFrame containing the core event data.
            actors (DataFrame): DataFrame containing actor (user) information.
            repo (DataFrame): DataFrame containing repository information.
            orgs (DataFrame): DataFrame containing organization data.
            payloads (DataFrame): DataFrame with extended GitHub event details.
    
        Returns:
            DataFrame: A processed and aggregated DataFrame ready for analysis.
        """
        events = (
            events.withColumn("ACTOR_ID", events["ACTOR_ID"].cast("string"))
            .withColumn("REPO_ID", events["REPO_ID"].cast("string"))
            .withColumn("ORG_ID", events["ORG_ID"].cast("string"))
            .withColumn("EVENT_ID", events["ID"].cast("string"))
            )
        actors = actors.withColumn("ID", actors["ID"].cast("string"))
        repo = repo.withColumn("ID", repo["ID"].cast("string"))
        orgs = orgs.withColumn("ID", orgs["ID"].cast("string"))
        payloads = payloads.withColumn("ID", payloads["ID"].cast("string"))
        
        # Join event data with actor data
        data = events.join(
            actors.select(
                actors["ID"].alias("ACTOR_ID_JOIN"),
                actors["LOGIN"]
            ),
            events["ACTOR_ID"] == actors["ID"],
            'left'
        )
        print('Events - Actors join')
        data.show()
        
        # Join with repo data
        data = data.join(
            repo.select(
                repo["ID"].alias("REPO_ID_JOIN"),
                repo["NAME"].alias("REPO_NAME")
            ),
            data["REPO_ID"] == repo["ID"],
            'left'
        )
        print('Data - Repo join')
        data.show()
        
        # Join with orgs data
        data = data.join(
            orgs.select(
                orgs["ID"].alias("ORG_ID_JOIN"),
                orgs["LOGIN"].alias("LOGIN_ORG")
            ),
            data["ORG_ID"] == orgs["ID"],
            'left'
        )
        print('Data - Orgs join')
        data.show()
        
        # Join with payloads data
        data = data.join(
            payloads,
            data["EVENT_ID"] == payloads["ID"],
            'left'
        )
        print('Data - Payloads join')
        data.show()
        
        # Convert timestamp columns
        data = data.withColumn('CREATED_AT', to_timestamp(col('CREATED_AT')))
        data = data.withColumn('PAYLOAD_CREATED_AT', to_timestamp(col('PAYLOAD_CREATED_AT')))
        data = data.withColumn('ISSUE_CLOSED_AT', to_timestamp(col('ISSUE_CLOSED_AT')))
        data = data.withColumn('RELEASE_PUBLISHED_AT', to_timestamp(col('RELEASE_PUBLISHED_AT')))
        data = data.withColumn('MERGE_AT', to_timestamp(col('MERGE_AT')))
        data = data.withColumn('REVIEW_SUBMITTED_AT', to_timestamp(col('REVIEW_SUBMITTED_AT')))

        # Create HAS_ISSUE column
        data = data.withColumn('HAS_ISSUE', col('ISSUE_URL').isNotNull())

        # Format date info, calculate times, identify bots, and format event types
        data = format_date_info(data)
        data = calculate_times(data)
        data = identify_bots(data)
        data = format_event_types(data)

        # Define columns for grouping
        group_columns = [
            'TYPE', 
            'LOGIN', 
            'REPO_NAME', 
            'PUBLIC',
            'LOGIN_ORG',
            'WEEKDAY_NUMBER_CREATED', 
            'WEEKDAY_CREATED', 
            'CREATED_HOUR', 
            'CREATED_AT',
            'IS_BOT',
            'LANGUAGE',
            'HAS_ISSUE'
        ]

        # Perform aggregations
        data_processed = data.group_by(*group_columns).agg(
            {'MERGE_TIME': 'sum', 
             'PR_REVIEW_TIME': 'sum', 
             'CLOSE_ISSUE_TIME': 'sum', 
             'PUBLISH_TIME': 'sum', 
             'TYPE': 'count'}
        )

        return data_processed

    def calculate_times(data):
        """
        Calculates various time-based metrics in seconds between event creation 
        and other key timestamps such as merge, review, close, and release.
    
        Args:
            data (DataFrame): DataFrame that includes timestamp columns like 
                              PAYLOAD_CREATED_AT, MERGE_AT, etc.
    
        Returns:
            DataFrame: The input DataFrame with added columns for each time metric.
        """
        data = data.withColumn(
            'MERGE_TIME', 
            datediff('second', col('PAYLOAD_CREATED_AT'), col('MERGE_AT'))
        )
        
        data = data.withColumn(
            'PR_REVIEW_TIME', 
            datediff('second', col('PAYLOAD_CREATED_AT'), col('REVIEW_SUBMITTED_AT'))
        )
        
        data = data.withColumn(
            'CLOSE_ISSUE_TIME', 
            datediff('second', col('PAYLOAD_CREATED_AT'), col('ISSUE_CLOSED_AT'))
        )
        
        data = data.withColumn(
            'PUBLISH_TIME', 
            datediff('second', col('PAYLOAD_CREATED_AT'), col('RELEASE_PUBLISHED_AT'))
        )

        return data

    def format_date_info(data):
        """
        Extracts and formats date-related information from event timestamps, 
        including the weekday name, weekday number, and the hour of event creation.
    
        Args:
            data (DataFrame): DataFrame containing the CREATED_AT timestamp column.
    
        Returns:
            DataFrame: The input DataFrame with added columns for weekday, hour, etc.
        """
        data = data.withColumn('WEEKDAY_NUMBER_CREATED', dayofweek(col('CREATED_AT')))
        data = data.withColumn('WEEKDAY_CREATED', dayname(col('CREATED_AT')))
        data = data.withColumn(
            'CREATED_HOUR',
            concat(
                hour(col('CREATED_AT')).cast('string'),
                lit(':00')
            )
        )
        data = data.withColumn('CREATED_AT', to_date(col('CREATED_AT')))

        return data

    def identify_bots(data):
        """
        Identifies whether an actor is a bot by checking if 'bot' appears in the login name.
    
        Args:
            data (DataFrame): DataFrame that includes a 'LOGIN' column.
    
        Returns:
            DataFrame: The input DataFrame with an added IS_BOT boolean column.
        """
        return data.withColumn(
            'IS_BOT',
            regexp_count(lower(col('LOGIN')), 'bot') > 0
        )

    def format_event_types(data):
        """
        Converts technical GitHub event type names into more readable labels using a mapping.
    
        Args:
            data (DataFrame): DataFrame containing a 'TYPE' column with GitHub event types.
    
        Returns:
            DataFrame: The input DataFrame with the 'TYPE' column updated to user-friendly labels.
        """
        replaces = {
            'CommitCommentEvent': 'Commit comment',
            'CreateEvent': 'Create',
            'DeleteEvent': 'Delete',
            'ForkEvent': 'Repository fork',
            'GollumEvent': 'Wiki edit',
            'IssueCommentEvent': 'Issue comment',
            'IssuesEvent': 'Issues',
            'MemberEvent': 'Member',
            'PublicEvent': 'Repository visibility changed',
            'PullRequestEvent': 'Pull request',
            'PullRequestReviewEvent': 'Pull request review',
            'PullRequestReviewCommentEvent': 'Pull request review comment',
            'PullRequestReviewThreadEvent': 'Pull request review thread',
            'PushEvent': 'Push to repository',
            'ReleaseEvent': 'Release published',
            'SponsorshipEvent': 'Sponsorship update',
            'WatchEvent': 'Repository starred',
        }
        
        # Create a CASE WHEN expression for replacing event types
        case_expr = None
        for original, replacement in replaces.items():
            if case_expr is None:
                case_expr = when(col('TYPE') == original, lit(replacement))
            else:
                case_expr = case_expr.when(col('TYPE') == original, lit(replacement))
        
        # Add the ELSE condition (keep original if not in the mapping)
        case_expr = case_expr.otherwise(col('TYPE'))
        
        # Apply the CASE WHEN expression
        data = data.withColumn('TYPE', case_expr)

        return data
    
    # Call the transform function
    result = transform_data(events_df, actor_df, repo_df, orgs_df, payloads_df)

    result.show()

    result.write.mode("overwrite").save_as_table("PROCESSED")
    
    # Return value will appear in the Results tab.
    return result