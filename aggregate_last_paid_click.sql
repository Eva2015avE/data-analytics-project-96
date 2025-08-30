WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        DATE(s.visit_date) AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY 
                CASE WHEN s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') 
                     THEN 1 ELSE 2 END,
                s.visit_date DESC
        ) AS rn
    FROM
        sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at
    WHERE
        s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

attributed_data AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(CASE 
                WHEN closing_reason = 'Успешно реализовано' OR status_id = 142 
                THEN lead_id 
            END) AS purchases_count,
        SUM(CASE 
                WHEN closing_reason = 'Успешно реализовано' OR status_id = 142 
                THEN amount 
            END) AS revenue
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
        DATE(campaign_date) AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM
        (
            SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
            UNION ALL
            SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
        ) AS all_ads
    GROUP BY
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
),

combined_data AS (
    SELECT
        attr.visit_date,
        attr.visitors_count,
        attr.utm_source,
        attr.utm_medium,
        attr.utm_campaign,
        COALESCE(ad.total_cost, 0) AS total_cost,
        attr.leads_count,
        attr.purchases_count,
        attr.revenue
    FROM
        attributed_data AS attr
    LEFT JOIN
        ad_costs AS ad
        ON attr.visit_date = ad.visit_date
        AND attr.utm_source = ad.utm_source
        AND attr.utm_medium = ad.utm_medium
        AND attr.utm_campaign = ad.utm_campaign

    UNION ALL

    SELECT
        ad.visit_date,
        0 AS visitors_count,
        ad.utm_source,
        ad.utm_medium,
        ad.utm_campaign,
        ad.total_cost,
        0 AS leads_count,
        0 AS purchases_count,
        0 AS revenue
    FROM
        ad_costs AS ad
    LEFT JOIN
        attributed_data AS attr
        ON ad.visit_date = attr.visit_date
        AND ad.utm_source = attr.utm_source
        AND ad.utm_medium = attr.utm_medium
        AND ad.utm_campaign = attr.utm_campaign
    WHERE
        attr.visit_date IS NULL
)

SELECT
    visit_date,
    visitors_count,
    utm_source,
    utm_medium,
    utm_campaign,
    total_cost,
    leads_count,
    purchases_count,
    revenue
FROM
    combined_data
ORDER BY
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC,
    revenue DESC NULLS LAST
LIMIT 15;
