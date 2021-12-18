WITH xxx_yyy_report AS (
    SELECT
        event_date as event_date,
        dimensiony as dimensiony,
        sum(metric_x) as metricx
    FROM reporting.cola_xxx_yyy_report
    GROUP BY 1,2
),
 foo_bar_report AS (
     SELECT
         d.date AS event_date,
         lower(dimensiony) AS dimensiony,
         SUM(nw.metricx / cr.usd_rate) AS metricx
     FROM metrics.foo_bar AS nw
              JOIN metrics.d_date AS d
              ON d.date_key = nw.datekey
              JOIN metrics.some_mapping AS cr
              ON cr.conversion_date = d.date
                AND cr.dimension_id = nw.dimensionkey
     WHERE nw.dimxid = 1
       AND nw.metricx > 0
       AND nw.othermetric > 0
     GROUP BY 1, 2
 ),
 checks AS (
     SELECT
         a.event_date as event_date,
         a.dimensiony as dimensiony,
         abs(round(a.metricx-b.metricx)) as col_c_rel_diff,
         abs((round(a.metricx-b.metricx)/a.metricx)*100) as metric_x_rel_diff
     FROM foo_bar_report a
     LEFT JOIN xxx_yyy_report b
     ON a.event_date = b.event_date
     AND a.dimensiony = b.dimensiony
 ),
errors AS (
    SELECT
        dimensiony,
        CONCAT(
            dimensiony , ' - ', CAST(col_c_rel_diff AS VARCHAR),
            '$ mismatch between metrics.foo_bar and reporting.cola_xxx_yyy_report on ',
            CAST(event_date AS VARCHAR)
        ) AS error_msg
    FROM checks
    WHERE
      event_date <= current_date - interval '2' day
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
        CONCAT(
            'mismatches for col_s_values [',
            array_join(array_agg(DISTINCT dimensiony), ','),
            '] in reporting.cola_xxx_yyy_report'
        ) AS short_message,
        array_join(
            array_agg(error_msg),
            CAST(chr(10) AS VARCHAR)
        ) AS detail_message
    FROM errors
    WHERE error_msg IS NOT NULL
)
SELECT * FROM messages WHERE short_message IS NOT NULL
