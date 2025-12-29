with customers as (
    select
        customer_id,
        signup_month
    from {{ ref('stg_customers') }}
),

subscriptions as (
    select
        customer_id,
        subscription_created_at,
        canceled_at
    from {{ ref('stg_subscriptions') }}
),

months as (
    select distinct
        payment_month as month
    from {{ ref('stg_payments') }}
),

cohort_activity as (
    select
        c.signup_month as cohort_month,
        m.month as activity_month,
        count(distinct c.customer_id) as active_customers
    from customers c
    join subscriptions s
        on c.customer_id = s.customer_id
    join months m
        on m.month >= c.signup_month
       and m.month >= date_trunc('month', s.subscription_created_at)
       and (s.canceled_at is null or m.month < date_trunc('month', s.canceled_at))
    group by cohort_month, activity_month
),

cohort_sizes as (
    select
        signup_month as cohort_month,
        count(distinct customer_id) as cohort_size
    from customers
    group by signup_month
),

final as (
    select
        a.cohort_month,
        a.activity_month,
        a.active_customers,
        s.cohort_size,

        a.active_customers
        / nullif(s.cohort_size, 0) as retention_pct
    from cohort_activity a
    join cohort_sizes s
        on a.cohort_month = s.cohort_month
)

select *
from final
order by cohort_month, activity_month
