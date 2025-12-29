with source as (
    select * from {{ source('raw', 'raw_products') }}
),

cleaned as (
    select
        product_id,
        product_name,
        product_type,
        billing_period,
        price,
        monthly_price,
        
        -- Product tier categorization
        case
            when product_name ilike '%starter%' then 'Starter'
            when product_name ilike '%professional%' or product_name ilike '%pro%' then 'Professional'
            when product_name ilike '%business%' then 'Business'
            when product_name ilike '%enterprise%' then 'Enterprise'
            when product_name ilike '%trial%' then 'Trial'
            else 'Other'
        end as product_tier
        
    from source
)

select * from cleaned