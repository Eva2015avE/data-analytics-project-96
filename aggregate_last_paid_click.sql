WITH 
-- Фильтруем только платные клики и форматируем дату
paid_sessions AS (
    SELECT
        visitor_id,
        TO_CHAR(visit_date, 'YYYY-MM-DD') AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign
    FROM
        sessions
    WHERE
        medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

-- Агрегируем данные по визитам
visits_agg AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT visitor_id) AS visitors_count
    FROM
        paid_sessions
    GROUP BY
        visit_date, utm_source, utm_medium, utm_campaign
),

-- Считаем затраты на рекламу из обеих рекламных систем с правильным форматом даты
costs_agg AS (
    SELECT
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
    ) AS all_ads
    GROUP BY
        TO_CHAR(campaign_date, 'YYYY-MM-DD'), utm_source, utm_medium, utm_campaign
),

-- Считаем лиды и покупки с правильным форматом даты
leads_agg AS (
    SELECT
        TO_CHAR(s.visit_date, 'YYYY-MM-DD') AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        COUNT(DISTINCT l.lead_id) AS leads_count,
        COUNT(DISTINCT CASE 
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
            THEN l.lead_id 
        END) AS purchases_count,
        SUM(CASE 
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
            THEN l.amount 
        END) AS revenue
    FROM
        sessions s
    LEFT JOIN
        leads l ON s.visitor_id = l.visitor_id 
        AND l.created_at >= s.visit_date
    GROUP BY
        TO_CHAR(s.visit_date, 'YYYY-MM-DD'), s.source, s.medium, s.campaign
)m_campaign) ASC;

-- Объединяем все метрики
SELECT
    v.visitors_count,
    c.total_cost,
    l.leads_count,
    l.purchases_count,
    l.revenue,
    COALESCE(v.visit_date, c.visit_date, l.visit_date) AS visit_date,
    COALESCE(v.utm_source, c.utm_source, l.utm_source) AS utm_source,
    COALESCE(v.utm_medium, c.utm_medium, l.utm_medium) AS utm_medium,
    COALESCE(v.utm_campaign, c.utm_campaign, l.utm_campaign) AS utm_campaign
FROM
    visits_agg AS v
FULL OUTER JOIN
    costs_agg AS c
    ON
        v.visit_date = c.visit_date
        AND v.utm_source = c.utm_source
        AND v.utm_medium = c.utm_medium
        AND v.utm_campaign = c.utm_campaign
FULL OUTER JOIN
    leads_agg AS l
    ON
        COALESCE(v.visit_date, c.visit_date) = l.visit_date
        AND COALESCE(v.utm_source, c.utm_source) = l.utm_source
        AND COALESCE(v.utm_medium, c.utm_medium) = l.utm_medium
        AND COALESCE(v.utm_campaign, c.utm_campaign) = l.utm_campaign
ORDER BY
    l.revenue DESC NULLS LAST,
    COALESCE(v.visit_date, c.visit_date, l.visit_date) ASC,
    v.visitors_count DESC,
    COALESCE(v.utm_source, c.utm_source, l.utm_source) ASC,
    COALESCE(v.utm_medium, c.utm_medium, l.utm_medium) ASC,
    COALESCE(v.utm_campaign, c.utm_campaign, l.utm_campaign) ASC
LIMIT 15;

