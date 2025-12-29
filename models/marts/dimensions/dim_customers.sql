with customers as (
    select *
    from {{ ref('stg_customers') }}
),

payments as (
    select *
    from {{ ref('int_customer_payments') }}
),

final as (
    select
        c.customer_id,
        c.email,
        c.country,
        c.customer_type,
        c.marketing_channel,

        c.customer_created_at,
        c.signup_month,
        c.signup_quarter,
        c.signup_year,

        coalesce(p.total_revenue, 0) as lifetime_revenue,
        coalesce(p.successful_payments, 0) as successful_payments,

        case
            when coalesce(p.total_revenue, 0) = 0 then 'No Revenue'
            when p.total_revenue < 500 then 'Low Value'
            when p.total_revenue < 2000 then 'Mid Value'
            else 'High Value'
        end as ltv_segment
    from customers c
    left join payments p
        on c.customer_id = p.customer_id
)

select * from final
