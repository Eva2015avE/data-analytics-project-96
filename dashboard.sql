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

--Время на закрытие лидов
SELECT
    s.source AS utm_source,
    s.campaign AS utm_campaign,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (
            l.created_at 
            - s.visit_date
        )) / 86400)::numeric,
        2
    ) AS avg_days_to_lead,
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
    s.source,
    s.campaign;

--Детальный анализ эффективности рекламных кампаний по
--источникам, каналам и конкретным кампаниям за июнь
--2023 года.
WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        DATE(s.visit_date) AS visit_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id
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
        utm_medium,
        utm_campaign,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(
            CASE
                WHEN closing_reason = 'Успешно
реализовано' OR status_id = 142
                THEN lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN closing_reason = 'Успешно
реализовано' OR status_id = 142
                THEN amount
            END
        ) AS revenue
    FROM
        last_paid_click
    WHERE
        rn = 1
    GROUP BY
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
),

ad_costs AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        DATE(campaign_date) AS visit_date,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM
            vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM
            ya_ads
    ) AS all_ads
    GROUP BY
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
),

marketing_data AS (
    SELECT
        attr.visit_date,
        attr.visitors_count,
        attr.leads_count,
        attr.purchases_count,
        attr.utm_source,
        attr.utm_medium,
        attr.utm_campaign,
        COALESCE(ad.total_cost, 0) AS total_cost,
        attr.revenue
    FROM attributed_data AS attr
    LEFT JOIN ad_costs AS ad
        ON attr.visit_date = ad.visit_date
        AND attr.utm_source = ad.utm_source
        AND attr.utm_medium = ad.utm_medium
        AND attr.utm_campaign = ad.utm_campaign
)

SELECT
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(visitors_count) AS visitors,
    SUM(leads_count) AS leads,
    SUM(purchases_count) AS purchases,
    SUM(total_cost) AS total_cost,
    SUM(revenue) AS total_revenue,
    ROUND(
        (SUM(revenue) - SUM(total_cost)) /
NULLIF(SUM(total_cost), 0) * 100,
        2
    ) AS roi,
    ROUND(
        SUM(total_cost) / NULLIF(SUM(purchases_count),
0),
        2
    ) AS cppu
FROM marketing_data
WHERE visit_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY
    utm_source,
    utm_medium,
    utm_campaign
ORDER BY
    roi DESC NULLS LAST;

--Сводный анализ эффективности рекламных каналов по
--источникам трафика за июнь 2023 года.
WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        DATE(s.visit_date) AS visit_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id
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
        utm_medium,
        utm_campaign,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(
            CASE
                WHEN closing_reason = 'Успешно
реализовано' OR status_id = 142
                THEN lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN closing_reason = 'Успешно
реализовано' OR status_id = 142
                THEN amount
            END
        ) AS revenue
    FROM
        last_paid_click
    WHERE
        rn = 1
    GROUP BY
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
),

ad_costs AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        DATE(campaign_date) AS visit_date,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM
            vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM
            ya_ads
    ) AS all_ads
    GROUP BY
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
),

marketing_data AS (
    SELECT
        attr.visit_date,
        attr.visitors_count,
        attr.leads_count,
        attr.purchases_count,
        attr.utm_source,
        COALESCE(ad.total_cost, 0) AS total_cost,
        attr.revenue
    FROM attributed_data AS attr
    LEFT JOIN ad_costs AS ad
        ON attr.visit_date = ad.visit_date
        AND attr.utm_source = ad.utm_source
        AND attr.utm_medium = ad.utm_medium
        AND attr.utm_campaign = ad.utm_campaign
)

SELECT
    utm_source,
    SUM(visitors_count) AS visitors,
    SUM(leads_count) AS leads,
    SUM(purchases_count) AS purchases,
    SUM(total_cost) AS total_cost,
    SUM(revenue) AS total_revenue,
    ROUND(
        SUM(total_cost) / NULLIF(SUM(visitors_count),
0),
        2
    ) AS cpu,
    ROUND(
        SUM(total_cost) / NULLIF(SUM(leads_count), 0),
        2
    ) AS cpl,
    ROUND(
        SUM(total_cost) / NULLIF(SUM(purchases_count),
0),
        2
    ) AS cppu,
    ROUND(
        (SUM(revenue) - SUM(total_cost)) /
NULLIF(SUM(total_cost), 0) * 100,
        2
    ) AS roi
FROM marketing_data
WHERE visit_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY
    utm_source
ORDER BY
    roi DESC NULLS LAST;


--Конверсионная воронка по источникам трафика за июнь
--2023 года.
WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        DATE(s.visit_date) AS visit_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id
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
        utm_medium,
        utm_campaign,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(
            CASE
                WHEN closing_reason = 'Успешно
реализовано' OR status_id = 142
                THEN lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN closing_reason = 'Успешно
реализовано' OR status_id = 142
                THEN amount
            END
        ) AS revenue
    FROM
        last_paid_click
    WHERE
        rn = 1
    GROUP BY
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
),

ad_costs AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        DATE(campaign_date) AS visit_date,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM
            vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM
            ya_ads
    ) AS all_ads
    GROUP BY
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
),

marketing_data AS (
    SELECT
        attr.visit_date,
        attr.visitors_count,
        attr.leads_count,
        attr.purchases_count,
        attr.utm_source,
        COALESCE(ad.total_cost, 0) AS total_cost,
        attr.revenue
    FROM attributed_data AS attr
    LEFT JOIN ad_costs AS ad
        ON attr.visit_date = ad.visit_date
        AND attr.utm_source = ad.utm_source
        AND attr.utm_medium = ad.utm_medium
        AND attr.utm_campaign = ad.utm_campaign
)

SELECT
    utm_source,
    SUM(visitors_count) AS visitors,
    SUM(leads_count) AS leads,
    SUM(purchases_count) AS purchases,
    ROUND(
        SUM(leads_count) * 100.0 /
NULLIF(SUM(visitors_count), 0),
        2
    ) AS visit_to_lead_conv,
    ROUND(
        SUM(purchases_count) * 100.0 / NULLIF(SUM(leads_count), 0),
        2
    ) AS lead_to_purchase_conv,
    ROUND(
        SUM(purchases_count) * 100.0 /
NULLIF(SUM(visitors_count), 0),
        2
    ) AS overall_conv
FROM marketing_data
WHERE visit_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY
    utm_source
ORDER BY
    visitors DESC;
