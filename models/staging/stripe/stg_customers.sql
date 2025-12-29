with source as(
    select * from {{ source('raw', 'raw_customers') }}
),
cleaned as(
    select
    customer_id,
    email,
    created_at as customer_created_at,
    country,
    customer_type,
    marketing_channel,

    date_trunc('month', created_at)::date as signup_month,
    date_trunc('quarter', created_at)::date as signup_quarter,
    extract(year from created_at) as signup_year
        
    from source
)

select * from cleaned