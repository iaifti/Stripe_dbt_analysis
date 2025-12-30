with cohorts as (
    select
        customer_id,
        cohort_month
    from {{ ref('int_customer_cohort') }}
),

subscriptions as (
    select
        customer_id,
        subscription_created_at,
        canceled_at
    from {{ ref('int_active_subscriptions') }}
),

months as (
    select distinct
        payment_month as activity_month
    from {{ ref('stg_payments') }}
),

cohort_activity as (
    select
        c.cohort_month,
        m.activity_month,
        count(distinct c.customer_id) as active_customers
    from cohorts c
    join subscriptions s
        on c.customer_id = s.customer_id
    join months m
        on m.activity_month >= c.cohort_month
       and m.activity_month >= date_trunc('month', s.subscription_created_at)
       and (
            s.canceled_at is null
            or m.activity_month < date_trunc('month', s.canceled_at)
       )
    group by 1, 2
),

cohort_sizes as (
    select
        cohort_month,
        count(*) as cohort_size
    from {{ ref('int_customer_cohort') }}
    group by cohort_month
),

final as (
    select
        a.cohort_month,
        a.activity_month,
        a.active_customers,
        s.cohort_size,
        a.active_customers / nullif(s.cohort_size, 0) as retention_pct
    from cohort_activity a
    join cohort_sizes s
        on a.cohort_month = s.cohort_month
)

select *
from final
order by cohort_month, activity_month