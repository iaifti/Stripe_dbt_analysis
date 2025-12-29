with source as(
    select * from {{ source('raw', 'raw_payments') }}
),

cleaned as(
    select
        payment_id,
        subscription_id,
        customer_id,
        amount,
        currency,
        status,
        payment_method,
        created_at as payment_date,
        failure_code,
        fee,

    amount - fee as net_amount,
        date_trunc('month', created_at)::date as payment_month,
        date_trunc('quarter', created_at)::date as payment_quarter,
        extract(year from created_at) as payment_year,
        
        -- Success flag
        case when status = 'succeeded' then true else false end as is_successful
        
    from source
)

select * from cleaned

