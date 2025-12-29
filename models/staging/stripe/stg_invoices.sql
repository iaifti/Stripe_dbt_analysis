with source as (
    select * from {{ source('raw', 'raw_invoices') }}
),

cleaned as (
    select
        invoice_id,
        subscription_id,
        customer_id,
        payment_id,
        amount_due,
        amount_paid,
        status,
        created_at as invoice_created_at,
        due_date,
        paid_at,
        
        -- Payment timing
        case
            when paid_at is not null then
                datediff(day, due_date, paid_at)
            else null
        end as days_to_payment,
        
        -- Status flags
        case when status = 'paid' then true else false end as is_paid,
        case when paid_at > due_date then true else false end as is_late_payment
        
    from source
)

select * from cleaned