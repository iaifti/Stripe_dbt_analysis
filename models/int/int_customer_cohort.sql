with customers as (
    select
        customer_id,
        signup_month
    from {{ ref('stg_customers') }}
),

cohorts as (
    select
        customer_id,
        signup_month as cohort_month
    from customers
)

select * from cohorts
