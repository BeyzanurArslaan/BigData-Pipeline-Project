select
    cast(order_id as string) as order_id,
    cast(payment_sequential as integer) as payment_sequential,
    cast(payment_type as string) as payment_type,
    cast(payment_installments as integer) as payment_installments,
    cast(payment_value as decimal(18, 2)) as payment_value
from {{ source('olist_bronze', 'order_payments') }}
