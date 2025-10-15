--Время на закрытие 90% лидов
SELECT
    s.source AS utm_source,
    ROUND(
        PERCENTILE_CONT(0.9) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (
                l.created_at
                - s.visit_date
            )) / 86400
        )::numeric,
        2
    ) AS percentile_90_days
FROM
    sessions AS s
INNER JOIN leads AS l
    ON s.visitor_id = l.visitor_id
WHERE
    (
        l.closing_reason = 'Успешно реализовано'
        OR l.status_id = 142
    )
    AND s.visit_date <= l.created_at
GROUP BY
    s.source;

--Анализ эффективности рекламных кампаний по источникам
WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.source AS utm_source,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        DATE(s.visit_date) AS visit_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM
        sessions AS s
    LEFT JOIN
        leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE
        s.medium IN (
            'cpc', 'cpm', 'cpa', 'youtube',
            'cpp', 'tg', 'social'
        )
),

attributed_data AS (
    SELECT
        visit_date,
        utm_source,
        COUNT(visitor_id) AS visitors_count,
        COUNT(
            CASE
                WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN amount
            END
        ) AS revenue
    FROM
        last_paid_click
    WHERE
        rn = 1
    GROUP BY
        visit_date,
        utm_source
),

ad_costs AS (
    SELECT
        utm_source,
        DATE(campaign_date) AS visit_date,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            daily_spent
        FROM
            vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            daily_spent
        FROM
            ya_ads
    ) AS all_ads
    GROUP BY
        DATE(campaign_date),
        utm_source
),

marketing_data AS (
    SELECT
        attr.visit_date,
        attr.visitors_count,
        attr.purchases_count,
        attr.utm_source,
        attr.revenue,
        COALESCE(ad.total_cost, 0) AS total_cost
    FROM
        attributed_data AS attr
    LEFT JOIN
        ad_costs AS ad
        ON
            attr.visit_date = ad.visit_date
            AND attr.utm_source = ad.utm_source
)

SELECT
    utm_source,
    SUM(visitors_count) AS visitors,
    SUM(purchases_count) AS purchases,
    SUM(total_cost) AS total_cost,
    SUM(revenue) AS total_revenue,
    ROUND(
        (SUM(revenue) - SUM(total_cost)) / NULLIF(SUM(total_cost), 0) * 100,
        2
    ) AS roi
FROM
    marketing_data
WHERE
    visit_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY
    utm_source
ORDER BY
    roi DESC NULLS LAST;
