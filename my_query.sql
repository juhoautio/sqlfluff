WITH xxx_yyy_report AS (
    SELECT
        event_date AS event_date,
        dimensiony AS dimensiony,
        sum(metric_x) AS metricx
    FROM reporting.cola_xxx_yyy_report
    GROUP BY 1, 2
),

foo_bar_report AS (
    SELECT
        metrics.d_date.date AS event_date,
        lower(dimensiony) AS dimensiony,
        sum(metrics.foo_bar.metricx / metrics.some_mapping.usd_rate) AS metricx
    FROM metrics.foo_bar
    INNER JOIN metrics.d_date
        ON metrics.d_date.date_key = metrics.foo_bar.datekey
    INNER JOIN metrics.some_mapping
        ON metrics.some_mapping.conversion_date = metrics.d_date.date
            AND metrics.some_mapping.dimension_id = metrics.foo_bar.dimensionkey
    WHERE metrics.foo_bar.dimxid = 1
        AND metrics.foo_bar.metricx > 0
        AND metrics.foo_bar.othermetric > 0
    GROUP BY 1, 2
),

checks AS (
    SELECT
        foo_bar_report.event_date AS event_date,
        foo_bar_report.dimensiony AS dimensiony,
        abs(
            round(foo_bar_report.metricx - xxx_yyy_report.metricx)
        ) AS col_c_rel_diff,
        abs(
            (
                round(
                    foo_bar_report.metricx - xxx_yyy_report.metricx
                ) / foo_bar_report.metricx
            ) * 100
        ) AS metric_x_rel_diff
    FROM foo_bar_report
    LEFT JOIN xxx_yyy_report
        ON foo_bar_report.event_date = xxx_yyy_report.event_date
            AND foo_bar_report.dimensiony = xxx_yyy_report.dimensiony
),

errors AS (
    SELECT
        dimensiony,
        concat(
            dimensiony, ' - ', cast(col_c_rel_diff AS VARCHAR),
            '$ mismatch between metrics.foo_bar and reporting.cola_xxx_yyy_report on ',
            cast(event_date AS VARCHAR)
        ) AS error_msg
    FROM checks
    WHERE
        event_date <= current_date - INTERVAL '2'   day
      AND
        (
            (dimensiony NOT IN ('a', 'b', 'c') AND col_c_rel_diff > 10 AND metric_x_rel_diff > 1)
            OR (dimensiony = 'a' AND col_c_rel_diff > 500)
            OR (dimensiony in ('b', 'c') AND col_c_rel_diff > 10)
        )
    ORDER BY event_date ASC
),

messages AS (
    SELECT
        concat(
            'mismatches for col_s_values [',
            array_join(array_agg(DISTINCT dimensiony), ','),
            '] in reporting.cola_xxx_yyy_report'
        ) AS short_message,
        array_join(
            array_agg(error_msg),
            cast(chr(10) AS VARCHAR)
        ) AS detail_message
    FROM errors
    WHERE error_msg IS NOT NULL
)

SELECT * FROM messages WHERE short_message IS NOT NULL
