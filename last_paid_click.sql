--Запрос для атрибуции лидов по модели Last Paid Click
WITH ranked_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        s.content AS utm_content,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY
                CASE
                    WHEN
                        s.medium IN (
                            'cpc',
                            'cpm',
                            'cpa',
                            'youtube',
                            'cpp',
                            'tg',
                            'social'
                        )
                        THEN 1
                    ELSE 2
                END,
                s.visit_date DESC
        ) AS rn
    FROM
        sessions AS s
    LEFT JOIN
        leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
)

SELECT
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM
    ranked_sessions
WHERE
    rn = 1
    AND utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;


