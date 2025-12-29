with subs as (
    select *
    from {{ ref('stg_subscriptions') }}
),

enhanced as (
    select
        subscription_id,
        customer_id,
        product_id,
        status,
        subscription_created_at,
        canceled_at,

        case
            when canceled_at is not null then canceled_at
            else current_date
        end as effective_end_date,

        datediff(
            day,
            subscription_created_at,
            case
                when canceled_at is not null then canceled_at
                else current_date
            end
        ) as lifetime_days

    from subs
)

select * from enhanced
