--ВСЕГО ПОСЕТИТЕЛЕЙ
SELECT COUNT(DISTINCT visitor_id) AS total_unique_visitors
FROM
    sessions;





--АКТИВНОСТЬ ПОЛЬЗОВАТЕЛЕЙ  (новые vs возвращающиеся)
WITH user_activity AS (
    SELECT
        visitor_id,
        MIN(visit_date) AS first_visit_date,
        COUNT(*) AS total_visits
    FROM
        sessions
    GROUP BY
        visitor_id
)

SELECT
    CASE
        WHEN
            first_visit_date >= CURRENT_DATE - INTERVAL '7 days'
            THEN 'New (last 7 days)'
        WHEN total_visits > 1 THEN 'Returning'
        ELSE 'One-time'
    END AS user_type,
    COUNT(*) AS users_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM
    user_activity
GROUP BY
    user_type;





-- СРЕДНЕЕ КОЛИЧЕСТВО ПОСЕЩЕНИЙ НА ПОЛЬЗОВАТЕЛЯ 
SELECT ROUND(AVG(visit_count), 1) AS avg_visits_per_user
FROM (
    SELECT
        visitor_id,
        COUNT(*) AS visit_count
    FROM
        sessions
    GROUP BY
        visitor_id
) AS user_visits;






--  КОЛИЧЕСТВО ПОСЕТИТЕЛЕЙ ПО ИСТОЧНИКАМ ТРАФИКА 
SELECT
    source AS traffic_source,
    COUNT(DISTINCT visitor_id) AS visitors_count,
    ROUND(
        100.0
        * COUNT(DISTINCT visitor_id)
        / SUM(COUNT(DISTINCT visitor_id)) OVER (),
        1
    ) AS percentage
FROM
    sessions
GROUP BY
    source
ORDER BY
    visitors_count DESC;







-- АНАЛИЗ ПО ИСТОЧНИКАМ ТРАФИКА 
SELECT
    s.source AS traffic_source,
    COUNT(DISTINCT s.visitor_id) AS visitors,
    COUNT(DISTINCT l.visitor_id) AS leads,
    COUNT(
        DISTINCT CASE WHEN l.status_id = 142 THEN l.visitor_id END
    ) AS paid_customers,
    ROUND(
        100.0
        * COUNT(DISTINCT l.visitor_id)
        / NULLIF(COUNT(DISTINCT s.visitor_id), 0),
        1
    ) AS visit_to_lead_rate,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN l.status_id = 142 THEN l.visitor_id END)
        / NULLIF(COUNT(DISTINCT l.visitor_id), 0), 1
    ) AS lead_to_paid_rate
FROM
    sessions AS s
LEFT JOIN
    leads AS l ON s.visitor_id = l.visitor_id
GROUP BY
    s.source
ORDER BY
    paid_customers DESC;




   
    

--АНАЛИЗ РЕКЛАМНЫХ РАСХОДОВ ПО НЕДЕЛЯМ И В МЕСЯЦ 
    WITH
-- 1. Объединяем данные рекламных расходов из всех источников
combined_ads AS (
    SELECT
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent
    FROM vk_ads
    WHERE utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')

    UNION ALL

    SELECT
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent
    FROM ya_ads
    WHERE utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

-- 2. Агрегируем расходы по неделям
weekly_costs AS (
    SELECT
        utm_source,
        utm_medium,
        DATE_TRUNC('week', campaign_date) AS week_start,
        SUM(daily_spent) AS weekly_cost,
        COUNT(DISTINCT campaign_date) AS days_active
    FROM combined_ads
    GROUP BY
        DATE_TRUNC('week', campaign_date),
        utm_source,
        utm_medium
),

-- 3. Агрегируем расходы по месяцам
monthly_costs AS (
    SELECT
        utm_source,
        utm_medium,
        DATE_TRUNC('month', campaign_date) AS month_start,
        SUM(daily_spent) AS monthly_cost,
        COUNT(DISTINCT campaign_date) AS days_active
    FROM combined_ads
    GROUP BY
        DATE_TRUNC('month', campaign_date),
        utm_source,
        utm_medium
)

-- 4. Финальный результат с объединением данных
SELECT
    'weekly' AS period_type,
    week_start AS period_start,
    utm_source,
    utm_medium,
    weekly_cost AS total_cost,
    days_active,
    ROUND(weekly_cost / 7, 2) AS avg_daily_cost,
    ROUND(weekly_cost / NULLIF(days_active, 0), 2) AS actual_daily_cost
FROM weekly_costs

UNION ALL

SELECT
    'monthly' AS period_type,
    month_start AS period_start,
    utm_source,
    utm_medium,
    monthly_cost AS total_cost,
    days_active,
    ROUND(monthly_cost / 30, 2) AS avg_daily_cost,
    ROUND(monthly_cost / NULLIF(days_active, 0), 2) AS actual_daily_cost
FROM monthly_costs

ORDER BY
    period_type DESC, -- Сначала месячные данные
    period_start ASC,
    utm_source ASC,
    utm_medium ASC;





-- СРЕДНИЙ ЧЕК ПО ИСТОЧНИКАМ 
WITH lead_stats AS (
    SELECT
        s.source AS traffic_source,
        l.amount,
        EXTRACT(DAY FROM (l.created_at - s.visit_date)) AS days_to_close
    FROM
        leads AS l
    INNER JOIN
        sessions AS s ON l.visitor_id = s.visitor_id
    WHERE
        l.status_id = 142  -- Оплаченные сделки
        AND s.source IN ('yandex', 'vk')
        AND l.amount > 0
)

SELECT
    traffic_source,
    COUNT(*) AS total_paid_leads,
    ROUND(AVG(amount), 2) AS avg_check,
    ROUND(MAX(amount), 2) AS max_check,
    ROUND(MIN(amount), 2) AS min_check,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_check,
    ROUND(AVG(days_to_close)) AS avg_days_to_close,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS traffic_share
FROM
    lead_stats
GROUP BY
    traffic_source
ORDER BY
    avg_check DESC;





--CPU
WITH 
-- 1. Объединяем данные рекламных расходов
combined_ads AS (
  SELECT
    utm_source,
    utm_medium,
    utm_campaign,
    DATE_TRUNC('day', campaign_date) AS cost_date,
    SUM(daily_spent) AS daily_spent
  FROM (
    SELECT utm_source, utm_medium, utm_campaign, campaign_date, daily_spent FROM vk_ads
    UNION ALL
    SELECT utm_source, utm_medium, utm_campaign, campaign_date, daily_spent FROM ya_ads
  ) AS ads
  GROUP BY utm_source, utm_medium, utm_campaign, DATE_TRUNC('day', campaign_date)
),

-- 2. Агрегируем данные сессий
session_metrics AS (
  SELECT
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    DATE_TRUNC('day', visit_date) AS visit_date,
    COUNT(DISTINCT visitor_id) AS visitors_count
  FROM sessions
  WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    AND source IN ('yandex', 'vk')
  GROUP BY source, medium, campaign, DATE_TRUNC('day', visit_date)
),

-- 3. Основной расчет с детализацией
detailed_metrics AS (
  SELECT
    COALESCE(ca.utm_source, sm.utm_source) AS utm_source,
    COALESCE(ca.utm_medium, sm.utm_medium) AS utm_medium,
    COALESCE(ca.utm_campaign, sm.utm_campaign) AS utm_campaign,
    SUM(COALESCE(ca.daily_spent, 0)) AS total_cost,
    SUM(COALESCE(sm.visitors_count, 0)) AS visitors_count,
    COUNT(DISTINCT COALESCE(ca.cost_date, sm.visit_date)) AS days_active,
    0 AS is_total_row -- Флаг для сортировки
  FROM combined_ads ca
  FULL OUTER JOIN session_metrics sm
    ON ca.utm_source = sm.utm_source
    AND ca.utm_medium = sm.utm_medium
    AND ca.utm_campaign = sm.utm_campaign
    AND ca.cost_date = sm.visit_date
  GROUP BY 
    COALESCE(ca.utm_source, sm.utm_source),
    COALESCE(ca.utm_medium, sm.utm_medium),
    COALESCE(ca.utm_campaign, sm.utm_campaign)
),

-- 4. Итоговые данные по источникам
source_totals AS (
  SELECT
    utm_source,
    'ALL' AS utm_medium,
    'ALL' AS utm_campaign,
    SUM(total_cost) AS total_cost,
    SUM(visitors_count) AS visitors_count,
    SUM(days_active) AS days_active,
    1 AS is_total_row -- Флаг для сортировки
  FROM detailed_metrics
  GROUP BY utm_source
)

-- 5. Финальный результат с расчетом CPU
SELECT
  utm_source AS traffic_source,
  utm_medium,
  utm_campaign,
  total_cost,
  visitors_count,
  -- Расчет CPU с защитой от деления на ноль
  CASE
    WHEN visitors_count = 0 THEN 0
    ELSE ROUND(total_cost / visitors_count, 2)
  END AS cpu,
  -- Дополнительные метрики
  days_active,
  ROUND(total_cost / NULLIF(days_active, 0), 2) AS avg_daily_cost,
  -- Доля посетителей от общего числа
  ROUND(visitors_count * 100.0 / NULLIF(SUM(visitors_count) OVER (PARTITION BY utm_source), 0), 2) AS percentage_of_source_visitors
FROM (
  SELECT * FROM detailed_metrics
  UNION ALL
  SELECT * FROM source_totals
) AS combined_data
ORDER BY 
  utm_source,
  is_total_row, -- Сначала детализированные данные, затем итоги
  utm_medium,
  utm_campaign;





--CPl 
WITH 
-- 1. Собираем платные клики (сессии)
paid_sessions AS (
  SELECT
    visitor_id,
    visit_date,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign
  FROM 
    sessions
  WHERE 
    medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    AND source IN ('yandex', 'vk')
),

-- 2. Агрегируем расходы по рекламным кампаниям
ad_costs AS (
  SELECT
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
  FROM (
    SELECT utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) AS combined_ads
  GROUP BY 
    utm_source, 
    utm_medium, 
    utm_campaign
),

-- 3. Считаем количество лидов по каждому каналу
lead_counts AS (
  SELECT
    s.utm_source,
    s.utm_medium,
    s.utm_campaign,
    COUNT(DISTINCT l.lead_id) AS leads_count
  FROM 
    paid_sessions s
  JOIN 
    leads l ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
  GROUP BY 
    s.utm_source,
    s.utm_medium,
    s.utm_campaign
)

-- 4. Основной запрос с детализацией по UTM-атрибутам
SELECT
  'by_source' AS aggregation_level,
  ac.utm_source,
  'ALL' AS utm_medium,
  'ALL' AS utm_campaign,
  SUM(ac.total_cost) AS total_cost,
  SUM(lc.leads_count) AS leads_count,
  CASE
    WHEN SUM(lc.leads_count) = 0 THEN 0
    ELSE ROUND(SUM(ac.total_cost) / SUM(lc.leads_count), 2)
  END AS cpl
FROM 
  ad_costs ac
JOIN 
  lead_counts lc ON ac.utm_source = lc.utm_source
GROUP BY 
  ac.utm_source

UNION ALL

-- 5. Детализированные данные по source + medium + campaign
SELECT
  'detailed' AS aggregation_level,
  COALESCE(ac.utm_source, lc.utm_source) AS utm_source,
  COALESCE(ac.utm_medium, lc.utm_medium) AS utm_medium,
  COALESCE(ac.utm_campaign, lc.utm_campaign) AS utm_campaign,
  COALESCE(ac.total_cost, 0) AS total_cost,
  COALESCE(lc.leads_count, 0) AS leads_count,
  CASE
    WHEN COALESCE(lc.leads_count, 0) = 0 THEN 0
    ELSE ROUND(COALESCE(ac.total_cost, 0) / COALESCE(lc.leads_count, 0), 2)
  END AS cpl
FROM 
  ad_costs ac
FULL OUTER JOIN 
  lead_counts lc ON ac.utm_source = lc.utm_source
                AND ac.utm_medium = lc.utm_medium
                AND ac.utm_campaign = lc.utm_campaign

ORDER BY
  aggregation_level DESC, -- Сначала агрегированные данные по source
  cpl DESC;





--CPPU и ROI
WITH 
-- 1. Собираем платные клики (сессии)
paid_sessions AS (
  SELECT
    visitor_id,
    visit_date,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign
  FROM 
    sessions
  WHERE 
    medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    AND source IN ('yandex', 'vk')
),

-- 2. Агрегируем расходы по рекламным кампаниям
ad_costs AS (
  SELECT
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
  FROM (
    SELECT utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) AS combined_ads
  GROUP BY 
    utm_source, 
    utm_medium, 
    utm_campaign
),

-- 3. Считаем количество покупок по каждому каналу
purchase_counts AS (
  SELECT
    s.utm_source,
    s.utm_medium,
    s.utm_campaign,
    COUNT(DISTINCT l.lead_id) AS purchases_count,
    SUM(l.amount) AS revenue
  FROM 
    paid_sessions s
  JOIN 
    leads l ON s.visitor_id = l.visitor_id 
           AND s.visit_date <= l.created_at
           AND (l.closing_reason = 'Успешно реализовано' OR l.status_id = 142)
  GROUP BY 
    s.utm_source,
    s.utm_medium,
    s.utm_campaign
),

-- 4. Объединяем данные для сортировки
combined_data AS (
  SELECT
    'by_source' AS aggregation_level,
    pc.utm_source,
    'ALL' AS utm_medium,
    'ALL' AS utm_campaign,
    COALESCE(SUM(ac.total_cost), 0) AS total_cost,
    COALESCE(SUM(pc.purchases_count), 0) AS purchases_count,
    COALESCE(SUM(pc.revenue), 0) AS revenue,
    CASE
      WHEN COALESCE(SUM(pc.purchases_count), 0) = 0 THEN 0
      ELSE ROUND(COALESCE(SUM(ac.total_cost), 0) / SUM(pc.purchases_count), 2)
    END AS cppu,
    CASE
      WHEN COALESCE(SUM(ac.total_cost), 0) = 0 THEN 0
      ELSE ROUND(COALESCE(SUM(pc.revenue), 0) / SUM(ac.total_cost), 2)
    END AS roi,
    1 AS sort_order
  FROM 
    purchase_counts pc
  LEFT JOIN 
    ad_costs ac ON pc.utm_source = ac.utm_source
  GROUP BY 
    pc.utm_source

  UNION ALL

  SELECT
    'detailed' AS aggregation_level,
    COALESCE(ac.utm_source, pc.utm_source) AS utm_source,
    COALESCE(ac.utm_medium, pc.utm_medium) AS utm_medium,
    COALESCE(ac.utm_campaign, pc.utm_campaign) AS utm_campaign,
    COALESCE(ac.total_cost, 0) AS total_cost,
    COALESCE(pc.purchases_count, 0) AS purchases_count,
    COALESCE(pc.revenue, 0) AS revenue,
    CASE
      WHEN COALESCE(pc.purchases_count, 0) = 0 THEN 0
      ELSE ROUND(COALESCE(ac.total_cost, 0) / pc.purchases_count, 2)
    END AS cppu,
    CASE
      WHEN COALESCE(ac.total_cost, 0) = 0 THEN 0
      ELSE ROUND(COALESCE(pc.revenue, 0) / ac.total_cost, 2)
    END AS roi,
    2 AS sort_order
  FROM 
    ad_costs ac
  FULL OUTER JOIN 
    purchase_counts pc ON ac.utm_source = pc.utm_source
                      AND ac.utm_medium = pc.utm_medium
                      AND ac.utm_campaign = pc.utm_campaign
)

-- 5. Финальный результат с правильной сортировкой
SELECT
  aggregation_level,
  utm_source,
  utm_medium,
  utm_campaign,
  total_cost,
  purchases_count,
  revenue,
  cppu,
  roi
FROM 
  combined_data
ORDER BY
  sort_order,
  CASE WHEN aggregation_level = 'by_source' THEN cppu ELSE 0 END DESC,
  cppu DESC;









   

      
    

