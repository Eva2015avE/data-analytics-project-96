-- Время закрытия лидов
SELECT
    s.source AS utm_source,
    s.campaign AS utm_campaign,
    AVG(
        EXTRACT(EPOCH FROM (l.created_at - s.visit_date)) / 86400
    ) AS avg_days_to_lead,
    PERCENTILE_CONT(0.9) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (l.created_at - s.visit_date)) / 86400
    ) AS percentile_90_days
FROM
    sessions AS s
INNER JOIN
    leads AS l
    ON s.visitor_id = l.visitor_id
WHERE
    (l.closing_reason = 'Успешно реализовано' OR l.status_id = 142)
    AND s.visit_date <= l.created_at
GROUP BY
    s.source,
    s.campaign;


-- Динамика пользователей и каналов
-- По дням (ВСЕ посетители)
SELECT
    source AS utm_source,
    DATE(visit_date) AS visit_date,
    COUNT(visitor_id) AS visitors_count
FROM
    sessions
WHERE
    DATE(visit_date) BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY
    DATE(visit_date),
    source
ORDER BY
    visit_date;


-- Запрос для конверсионной воронки
WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        DATE(s.visit_date) AS visit_date,
        ROW_NUMBER()
            OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
            AS rn
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

attributed_data AS (
    SELECT
        visit_date,
        utm_source,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(
            CASE
                WHEN
                    closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN
                    closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN amount
            END
        ) AS revenue
    FROM last_paid_click
    WHERE rn = 1
    GROUP BY visit_date, utm_source
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
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            daily_spent
        FROM ya_ads
    ) AS all_ads
    GROUP BY DATE(campaign_date), utm_source
)

SELECT
    utm_source,
    SUM(visitors_count) AS visitors,
    SUM(leads_count) AS leads,
    SUM(purchases_count) AS purchases,
    ROUND(SUM(leads_count) * 100.0 / NULLIF(SUM(visitors_count), 0), 2)
        AS visit_to_lead_conv,
    ROUND(SUM(purchases_count) * 100.0 / NULLIF(SUM(leads_count), 0), 2)
        AS lead_to_purchase_conv,
    ROUND(SUM(purchases_count) * 100.0 / NULLIF(SUM(visitors_count), 0), 2)
        AS overall_conv
FROM attributed_data AS attr
LEFT JOIN
    ad_costs AS ad
    ON attr.visit_date = ad.visit_date AND attr.utm_source = ad.utm_source
WHERE attr.visit_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY utm_source
ORDER BY visitors DESC;


-- Запрос для детального анализа по кампаниям
WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        DATE(s.visit_date) AS visit_date,
        ROW_NUMBER()
            OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
            AS rn
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

attributed_data AS (
    SELECT
        visit_date,
        utm_source,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(
            CASE
                WHEN
                    closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN
                    closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN amount
            END
        ) AS revenue
    FROM last_paid_click
    WHERE rn = 1
    GROUP BY visit_date, utm_source
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
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            daily_spent
        FROM ya_ads
    ) AS all_ads
    GROUP BY DATE(campaign_date), utm_source
)

SELECT
    utm_source,
    SUM(visitors_count) AS visitors,
    SUM(leads_count) AS leads,
    SUM(purchases_count) AS purchases,
    ROUND(SUM(leads_count) * 100.0 / NULLIF(SUM(visitors_count), 0), 2)
        AS visit_to_lead_conv,
    ROUND(SUM(purchases_count) * 100.0 / NULLIF(SUM(leads_count), 0), 2)
        AS lead_to_purchase_conv,
    ROUND(SUM(purchases_count) * 100.0 / NULLIF(SUM(visitors_count), 0), 2)
        AS overall_conv
FROM attributed_data AS attr
LEFT JOIN
    ad_costs AS ad
    ON attr.visit_date = ad.visit_date AND attr.utm_source = ad.utm_source
WHERE attr.visit_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY utm_source
ORDER BY visitors DESC;


-- Запрос для анализа затрат и окупаемости
WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        DATE(s.visit_date) AS visit_date,
        ROW_NUMBER()
            OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
            AS rn
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

attributed_data AS (
    SELECT
        visit_date,
        utm_source,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(
            CASE
                WHEN
                    closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN
                    closing_reason = 'Успешно реализовано' OR status_id = 142
                    THEN amount
            END
        ) AS revenue
    FROM last_paid_click
    WHERE rn = 1
    GROUP BY visit_date, utm_source
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
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            daily_spent
        FROM ya_ads
    ) AS all_ads
    GROUP BY DATE(campaign_date), utm_source
)

SELECT
    utm_source,
    SUM(COALESCE(ad.total_cost, 0)) AS total_cost,
    SUM(attr.revenue) AS total_revenue,
    SUM(attr.visitors_count) AS visitors,
    SUM(attr.leads_count) AS leads,
    SUM(attr.purchases_count) AS purchases,
    ROUND(
        SUM(COALESCE(ad.total_cost, 0)) / NULLIF(SUM(attr.visitors_count), 0), 2
    ) AS cpu,
    ROUND(
        SUM(COALESCE(ad.total_cost, 0)) / NULLIF(SUM(attr.leads_count), 0), 2
    ) AS cpl,
    ROUND(
        SUM(COALESCE(ad.total_cost, 0)) / NULLIF(SUM(attr.purchases_count), 0),
        2
    ) AS cppu,
    ROUND(
        (SUM(attr.revenue) - SUM(COALESCE(ad.total_cost, 0)))
        / NULLIF(SUM(COALESCE(ad.total_cost, 0)), 0)
        * 100,
        2
    ) AS roi
FROM attributed_data AS attr
LEFT JOIN
    ad_costs AS ad
    ON attr.visit_date = ad.visit_date AND attr.utm_source = ad.utm_source
WHERE attr.visit_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY utm_source
ORDER BY roi DESC NULLS LAST;