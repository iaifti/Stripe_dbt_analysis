with base as (
    select
        payment_month,
        net_revenue as mrr
    from {{ ref('int_monthly_revenue') }}
),

with_previous as (
    select
        payment_month,
        mrr,
        lag(mrr) over (order by payment_month) as previous_mrr
    from base
),

final as (
    select
        payment_month,
        mrr,
        previous_mrr,
        case
            when previous_mrr is null then null
            else round(100 * (mrr - previous_mrr) / previous_mrr, 2)
        end as mom_growth
    from with_previous
)

select *
from final
order by payment_month
