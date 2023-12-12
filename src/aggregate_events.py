import sqlalchemy as sa


def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    activity_periods_stmt = sa.text(
        """
        -- categorize events into 'people' or 'vehicles'
        WITH event_periods AS (
            SELECT
                *,
                LAG(time) OVER (PARTITION BY group_type ORDER BY time) AS prev_time
            FROM (
                SELECT
                    CASE
                        WHEN type IN ('pedestrian', 'bicycle') THEN 'people'
                        ELSE 'vehicles'
                    END AS group_type,
                    time
                FROM
                    events
            ) categorise_events
        )
        SELECT
            group_type,
            TO_CHAR(MIN(time), 'YYYY-MM-DD"T"HH24:MI:SS') AS start_time,
            TO_CHAR(MAX(time), 'YYYY-MM-DD"T"HH24:MI:SS') AS end_time
        FROM (
            -- use incrementing SUM to trigger new activity_period
            SELECT
                group_type,
                time,
                SUM(is_new_period) OVER (PARTITION BY group_type ORDER BY time) AS activity_period
            FROM (
                SELECT
                    group_type,
                    time,
                    CASE
                        WHEN prev_time IS NULL OR time - prev_time > INTERVAL '1 minute' THEN 1
                        ELSE 0
                    END AS is_new_period
                FROM
                    event_periods
            ) determine_intervals
        ) calculate_activity_periods
        GROUP BY
            group_type, activity_period
        ORDER BY
            group_type, start_time;
        """
    )
    activity_periods_result = conn.execute(activity_periods_stmt)

    activity_periods: dict[str, list[tuple[str, str]]] = {"people": [], "vehicles": []}

    for row in activity_periods_result:
        activity_periods[row.group_type].append(
            (str(row.start_time), str(row.end_time))
        )

    return activity_periods
