WITH paid_sessions AS (
    SELECT 
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        s.content AS utm_content,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id 
            ORDER BY 
                CASE WHEN s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') 
                THEN 0 ELSE 1 END,
                s.visit_date DESC
        ) AS rn
    FROM 
        sessions s
),

last_paid_click AS (
    SELECT 
        ps.visitor_id,
        ps.visit_date,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        ps.utm_content
    FROM 
        paid_sessions ps
    WHERE 
        ps.rn = 1
        AND ps.utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

SELECT 
    lpc.visitor_id,
    lpc.visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM 
    last_paid_click lpc
LEFT JOIN 
    leads l ON lpc.visitor_id = l.visitor_id
    AND l.created_at >= lpc.visit_date
ORDER BY 
    l.amount DESC NULLS LAST,
    lpc.visit_date ASC,
    lpc.utm_source ASC,
    lpc.utm_medium ASC,
    lpc.utm_campaign ASC;


WITH paid_sessions AS (
    SELECT 
        visitor_id,
        visit_date::date AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        content AS utm_content,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id 
            ORDER BY 
                CASE WHEN medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') 
                THEN 0 ELSE 1 END,
                visit_date DESC
        ) AS rn
    FROM 
        sessions
),

last_paid_click AS (
    SELECT 
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM 
        paid_sessions
    WHERE 
        rn = 1
        AND utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

ad_costs_aggregated AS (
    SELECT 
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_spent
    FROM (
        SELECT 
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT 
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            daily_spent
        FROM ya_ads
    ) AS combined_ads
    GROUP BY 
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign
),

leads_metrics AS (
    SELECT 
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
        COUNT(DISTINCT l.lead_id) AS leads_count,
        COUNT(DISTINCT CASE 
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
            THEN l.lead_id 
        END) AS purchases_count,
        SUM(CASE 
            WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
            THEN l.amount ELSE 0 
        END) AS revenue
    FROM last_paid_click lpc
    LEFT JOIN leads l 
        ON lpc.visitor_id = l.visitor_id
        AND l.created_at >= lpc.visit_date
    GROUP BY 
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign
)

SELECT 
    lm.visit_date,
    lm.utm_source,
    lm.utm_medium,
    lm.utm_campaign,
    lm.visitors_count,
    COALESCE(aca.daily_spent, 0) AS total_cost,
    lm.leads_count,
    lm.purchases_count,
    lm.revenue
FROM leads_metrics lm
LEFT JOIN ad_costs_aggregated aca
    ON lm.visit_date = aca.visit_date
    AND lm.utm_source = aca.utm_source
    AND lm.utm_medium = aca.utm_medium
    AND lm.utm_campaign = aca.utm_campaign
ORDER BY 
    lm.revenue DESC NULLS LAST,
    lm.visit_date ASC,
    lm.visitors_count DESC,
    lm.utm_source ASC,
    lm.utm_medium ASC,
    lm.utm_campaign asc
limit 15;

