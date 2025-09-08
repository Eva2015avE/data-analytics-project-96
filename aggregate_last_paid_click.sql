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
            ORDER BY
                s.visit_date DESC
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
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
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
                WHEN
                    closing_reason = 'Успешно реализовано'
                    OR status_id = 142
                    THEN
                        lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN
                    closing_reason = 'Успешно реализовано'
                    OR status_id = 142
                    THEN
                        amount
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
)

SELECT
    attr.visit_date,
    attr.visitors_count,
    attr.utm_source,
    attr.utm_medium,
    attr.utm_campaign,
    ad.total_cost,
    attr.leads_count,
    attr.purchases_count,
    attr.revenue
FROM
    attributed_data AS attr
LEFT JOIN
    ad_costs AS ad
    ON
        attr.visit_date = ad.visit_date
        AND attr.utm_source = ad.utm_source
        AND attr.utm_medium = ad.utm_medium
        AND attr.utm_campaign = ad.utm_campaign
ORDER BY
    attr.revenue DESC NULLS LAST
LIMIT 15;
