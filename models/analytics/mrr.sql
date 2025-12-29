with monthly_revenue as (
    select
        payment_month,
        sum(amount) as mrr
    from {{ ref('stg_payments') }}
    where is_successful = true
    group by payment_month
),

with_previous as (
    select
        payment_month,
        mrr,
        lag(mrr) over (order by payment_month) as previous_mrr
    from monthly_revenue
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
