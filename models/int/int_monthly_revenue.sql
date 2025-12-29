with payments as (
    select *
    from {{ ref('stg_payments') }}
    where is_successful = true
),

monthly as (
    select
        payment_month,
        sum(amount) as gross_revenue,
        sum(net_amount) as net_revenue,
        count(*) as successful_payments

    from payments
    group by payment_month
)

select * from monthly