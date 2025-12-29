with source as (
    select * from {{ source('raw', 'raw_subscriptions') }}
),

cleaned as (
    select
        subscription_id,
        customer_id,
        product_id,
        status,
        created_at as subscription_created_at,
        current_period_start,
        current_period_end,
        canceled_at,
        trial_start,
        trial_end,
        
        -- Subscription duration calculations
        case
            when canceled_at is not null then
                datediff(day, created_at, canceled_at)
            else
                datediff(day, created_at, current_date)
        end as subscription_duration_days,
        
        -- Status flags
        case when canceled_at is not null then true else false end as is_canceled,
        case when trial_start is not null then true else false end as had_trial
        
    from source
)

select * from cleaned