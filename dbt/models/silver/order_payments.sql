with deduped_order_payments as (
    select
        cast(order_id as string) as order_id,
        cast(payment_sequential as integer) as payment_sequential,
        cast(payment_type as string) as payment_type,
        cast(payment_installments as integer) as payment_installments,
        cast(payment_value as decimal(18, 2)) as payment_value,
        row_number() over (
            partition by cast(order_id as string), cast(payment_sequential as integer)
            order by
                cast(payment_type as string) asc nulls last,
                cast(payment_installments as integer) asc nulls last,
                cast(payment_value as decimal(18, 2)) desc nulls last
        ) as row_number
    from {{ ref('stg_order_payments') }}
)

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    cast(
        sum(coalesce(payment_value, cast(0 as decimal(18, 2))))
            over (partition by order_id)
        as decimal(18, 2)
    ) as order_payment_value,
    cast(count(*) over (partition by order_id) as integer) as payment_record_count,
    first_value(payment_type) over (
        partition by order_id
        order by
            payment_sequential asc nulls last,
            payment_type asc nulls last,
            payment_installments asc nulls last,
            payment_value desc nulls last
    ) as primary_payment_type,
    first_value(payment_installments) over (
        partition by order_id
        order by
            payment_sequential asc nulls last,
            payment_type asc nulls last,
            payment_installments asc nulls last,
            payment_value desc nulls last
    ) as primary_payment_installments
from deduped_order_payments
where row_number = 1
