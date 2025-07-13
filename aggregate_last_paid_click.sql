WITH 
-- 1. Собираем платные клики (сессии) по нужным рекламным каналам
paid_sessions AS (
  SELECT
    visitor_id,
    visit_date,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    content AS utm_content,
    landing_page
  FROM 
    sessions
  WHERE 
    medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    AND source IN ('yandex', 'vk')  -- Только платные источники
),
-- 2. Находим последний платный клик для каждого лида (без QUALIFY)
last_paid_click_temp AS (
  SELECT
    l.lead_id,
    l.visitor_id,  -- Добавляем visitor_id в выборку
    l.amount,
    l.status_id,
    l.closing_reason,
    l.created_at AS lead_date,
    p.visit_date,
    p.utm_source,
    p.utm_medium,
    p.utm_campaign,
    p.utm_content,
    ROW_NUMBER() OVER (PARTITION BY l.lead_id ORDER BY p.visit_date DESC) AS rn
  FROM 
    leads l
  JOIN 
    paid_sessions p ON l.visitor_id = p.visitor_id
  WHERE 
    p.visit_date <= l.created_at
),

last_paid_click AS (
  SELECT
    lead_id,
    visitor_id,  -- Сохраняем visitor_id в итоговом CTE
    amount,
    status_id,
    closing_reason,
    lead_date,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content
  FROM 
    last_paid_click_temp
  WHERE 
    rn = 1  -- Берем только последний клик
),
-- 3. Суммируем расходы по рекламным кампаниям
ad_costs AS (
  SELECT
    DATE_TRUNC('day', campaign_date) AS cost_date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
  FROM (
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) AS combined_ads
  GROUP BY 
    DATE_TRUNC('day', campaign_date), utm_source, utm_medium, utm_campaign
)
-- 4. Собираем итоговую витрину
SELECT
    ps.utm_source,
    ps.utm_medium,
    ps.utm_campaign,
    DATE_TRUNC('day', ps.visit_date) AS visit_date,
    COUNT(DISTINCT ps.visitor_id) AS visitors_count,
    COALESCE(SUM(ac.total_cost), 0) AS total_cost,
    COUNT(DISTINCT lpc.lead_id) AS leads_count,
    COUNT(DISTINCT CASE
        WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142
            THEN lpc.lead_id
    END) AS purchases_count,
    SUM(CASE
        WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142
            THEN lpc.amount
        ELSE 0
    END) AS revenue
FROM
    paid_sessions AS ps
LEFT JOIN
    last_paid_click AS lpc
    ON
        ps.visitor_id = lpc.visitor_id
        AND DATE_TRUNC('day', ps.visit_date) = DATE_TRUNC('day', lpc.visit_date)
        AND ps.utm_source = lpc.utm_source
        AND ps.utm_medium = lpc.utm_medium
        AND ps.utm_campaign = lpc.utm_campaign
LEFT JOIN
    ad_costs AS ac
    ON
        DATE_TRUNC('day', ps.visit_date) = ac.cost_date
        AND ps.utm_source = ac.utm_source
        AND ps.utm_medium = ac.utm_medium
        AND ps.utm_campaign = ac.utm_campaign
GROUP BY
    DATE_TRUNC('day', ps.visit_date),
    ps.utm_source,
    ps.utm_medium,
    ps.utm_campaign
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC;