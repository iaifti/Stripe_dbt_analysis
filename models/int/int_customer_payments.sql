with payments as (
    select *
    from {{ ref('stg_payments') }}
    where is_successful = true
),

aggregated as (
    select
        customer_id,

        count(*) as successful_payments,
        sum(amount) as total_revenue,
        sum(net_amount) as net_revenue,

        min(payment_date) as first_payment_date,
        max(payment_date) as last_payment_date

    from payments
    group by customer_id
)

select * from aggregated
