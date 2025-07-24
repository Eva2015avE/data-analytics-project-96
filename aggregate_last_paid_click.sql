WITH 
-- Фильтруем только указанные платные каналы
paid_sessions AS (
    SELECT
        visitor_id,
        CAST(visit_date AS DATE) AS visit_date,
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

-- Считаем затраты только для указанных платных каналов
costs_agg AS (
    SELECT
        CAST(campaign_date AS DATE) AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        FLOOR(SUM(daily_spent)) AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent 
        FROM vk_ads
        WHERE utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        
        UNION ALL
        
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent 
        FROM ya_ads
        WHERE utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    ) AS all_ads
    GROUP BY
        CAST(campaign_date AS DATE), utm_source, utm_medium, utm_campaign
),

-- Считаем лиды и покупки только от указанных платных каналов
leads_agg AS (
    SELECT
        ps.visit_date,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        COUNT(DISTINCT l.lead_id) AS leads_count,
        COUNT(DISTINCT CASE 
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
            THEN l.lead_id 
        END) AS purchases_count,
        FLOOR(SUM(CASE 
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
            THEN l.amount 
        END)) AS revenue
    FROM
        paid_sessions ps
    LEFT JOIN
        leads l ON ps.visitor_id = l.visitor_id 
        AND l.created_at >= ps.visit_date
    GROUP BY
        ps.visit_date, ps.utm_source, ps.utm_medium, ps.utm_campaign
)

-- Итоговый результат
SELECT
    COALESCE(v.visit_date, c.visit_date, l.visit_date) AS visit_date,
    COALESCE(v.visitors_count, 0) AS visitors_count,
    COALESCE(v.utm_source, c.utm_source, l.utm_source) AS utm_source,
    COALESCE(v.utm_medium, c.utm_medium, l.utm_medium) AS utm_medium,
    COALESCE(v.utm_campaign, c.utm_campaign, l.utm_campaign) AS utm_campaign,
    COALESCE(c.total_cost, 0) AS total_cost,
    COALESCE(l.leads_count, 0) AS leads_count,
    COALESCE(l.purchases_count, 0) AS purchases_count,
    COALESCE(l.revenue, 0) AS revenue
FROM
    visits_agg v
FULL OUTER JOIN
    costs_agg c ON v.visit_date = c.visit_date 
    AND v.utm_source = c.utm_source 
    AND v.utm_medium = c.utm_medium 
    AND v.utm_campaign = c.utm_campaign
FULL OUTER JOIN
    leads_agg l ON COALESCE(v.visit_date, c.visit_date) = l.visit_date
    AND COALESCE(v.utm_source, c.utm_source) = l.utm_source
    AND COALESCE(v.utm_medium, c.utm_medium) = l.utm_medium
    AND COALESCE(v.utm_campaign, c.utm_campaign) = l.utm_campaign
ORDER BY
    COALESCE(l.revenue, 0) DESC NULLS LAST,
    COALESCE(v.visit_date, c.visit_date, l.visit_date) ASC,
    COALESCE(v.visitors_count, 0) DESC,
    COALESCE(v.utm_source, c.utm_source, l.utm_source) ASC,
    COALESCE(v.utm_medium, c.utm_medium, l.utm_medium) ASC,
    COALESCE(v.utm_campaign, c.utm_campaign, l.utm_campaign) asc
    limit 15;

