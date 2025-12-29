with subs as (
    select *
    from {{ ref('int_active_subscriptions') }}
)

select
    subscription_id,
    customer_id,
    product_id,

    subscription_created_at,
    canceled_at,
    effective_end_date,
    lifetime_days,
    status
from subs
