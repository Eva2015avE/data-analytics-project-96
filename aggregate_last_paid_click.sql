WITH ranked_sessions AS (
    SELECT
        visitor_id,
        visit_date::date AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id
            ORDER BY visit_date DESC
        ) AS rn
    FROM sessions
    WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

last_paid_click AS (
    SELECT *
    FROM ranked_sessions
    WHERE rn = 1
),

ad_costs AS (
    SELECT
        campaign_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM ya_ads
    ) AS ads
    GROUP BY visit_date, utm_source, utm_medium, utm_campaign
),

leads_agg AS (
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
                THEN l.amount
            ELSE 0
        END) AS revenue
    FROM last_paid_click AS lpc
    LEFT JOIN
        leads AS l
        ON lpc.visitor_id = l.visitor_id AND lpc.visit_date <= l.created_at
    GROUP BY lpc.visit_date, lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
)

SELECT
    la.visit_date,
    la.utm_source,
    la.utm_medium,
    la.utm_campaign,
    la.visitors_count,
    la.leads_count,
    la.purchases_count,
    la.revenue,
    COALESCE(ac.total_cost, 0) AS total_cost
FROM leads_agg AS la
LEFT JOIN ad_costs AS ac
    ON
        la.visit_date = ac.visit_date
        AND la.utm_source = ac.utm_source
        AND la.utm_medium = ac.utm_medium
        AND la.utm_campaign = ac.utm_campaign
ORDER BY
    la.revenue DESC NULLS LAST,
    la.visit_date ASC,
    la.visitors_count DESC,
    la.utm_source ASC,
    la.utm_medium ASC,
    la.utm_campaign ASC
LIMIT 15;

