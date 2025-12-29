with months as (
    -- all months we care about
    select distinct
        payment_month as month
    from {{ ref('stg_payments') }}
),

subscriptions as (
    select
        subscription_id,
        subscription_created_at,
        canceled_at
    from {{ ref('stg_subscriptions') }}
),

active_at_start as (
    -- subscriptions active at the start of each month
    select
        m.month,
        count(distinct s.subscription_id) as active_subscriptions
    from months m
    join subscriptions s
        on s.subscription_created_at < m.month
       and (s.canceled_at is null or s.canceled_at >= m.month)
    group by m.month
),

churned_in_month as (
    -- subscriptions that canceled during the month
    select
        date_trunc('month', canceled_at)::date as month,
        count(distinct subscription_id) as churned_subscriptions
    from subscriptions
    where canceled_at is not null
    group by month
),

final as (
    select
        a.month,
        a.active_subscriptions,
        coalesce(c.churned_subscriptions, 0) as churned_subscriptions,

        coalesce(c.churned_subscriptions, 0)
        / nullif(a.active_subscriptions, 0) as churn_rate
    from active_at_start a
    left join churned_in_month c
        on a.month = c.month
)

select *
from final
order by month
