select
    product_id,
    product_name,
    product_type,
    billing_period,
    price,
    monthly_price,
    product_tier
from {{ ref('stg_orders') }}
