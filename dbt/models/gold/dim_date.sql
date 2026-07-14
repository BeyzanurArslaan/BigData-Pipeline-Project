with order_dates as (
    select distinct
        cast(date_format(cast(order_purchase_timestamp as date), 'yyyyMMdd') as integer) as date_key,
        cast(order_purchase_timestamp as date) as full_date,
        year(cast(order_purchase_timestamp as date)) as year,
        quarter(cast(order_purchase_timestamp as date)) as quarter,
        month(cast(order_purchase_timestamp as date)) as month,
        date_format(cast(order_purchase_timestamp as date), 'MMMM') as month_name,
        day(cast(order_purchase_timestamp as date)) as day,
        dayofweek(cast(order_purchase_timestamp as date)) as day_of_week,
        date_format(cast(order_purchase_timestamp as date), 'EEEE') as day_name,
        case
            when dayofweek(cast(order_purchase_timestamp as date)) in (1, 7) then true
            else false
        end as is_weekend
    from {{ ref('orders') }}
    where order_purchase_timestamp is not null
)

select
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    is_weekend
from order_dates
