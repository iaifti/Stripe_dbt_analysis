with payments as (
    select *
    from {{ ref('stg_payments') }}
),

final as (
    select
        payment_id,
        subscription_id,
        customer_id,

        payment_date,
        payment_month,
        payment_quarter,
        payment_year,

        amount,
        net_amount,
        fee,

        currency,
        payment_method,
        is_successful
    from payments
)

select * from final
