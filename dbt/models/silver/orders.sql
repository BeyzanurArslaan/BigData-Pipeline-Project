with source_orders as (
    select
        cast(order_id as string) as order_id,
        cast(customer_id as string) as customer_id,
        cast(order_status as string) as order_status,
        cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        cast(order_approved_at as timestamp) as order_approved_at,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
        cast(date_format(cast(order_purchase_timestamp as timestamp), 'yyyyMMdd') as integer) as order_purchase_date_key,
        row_number() over (
            partition by cast(order_id as string)
            order by
                cast(order_purchase_timestamp as timestamp) asc nulls last,
                cast(order_approved_at as timestamp) asc nulls last,
                cast(order_delivered_carrier_date as timestamp) asc nulls last,
                cast(order_delivered_customer_date as timestamp) asc nulls last,
                cast(order_estimated_delivery_date as timestamp) asc nulls last
        ) as row_number
    from {{ ref('stg_orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_purchase_date_key
from source_orders
where row_number = 1
