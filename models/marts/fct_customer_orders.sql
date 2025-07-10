with

customers as (
    select
    ID as customer_id, 
    FIRST_NAME as first_name,
    LAST_NAME as last_name
    from {{ source('raw', 'customers') }}
)

, orders as (
    select 
        ID as order_id,
        USER_ID as customer_id,
        ORDER_DATE as order_placed_at,
        STATUS as order_status
    from {{ source('raw', 'orders') }}
)

, payments as (
    select
        ORDERID as order_id,
        CREATED as payment_date,
        AMOUNT as payment_amount,
        STATUS as payment_status
    from {{ source('raw', 'payments') }}
)

, agg_payments as (
    select
        payments.order_id,
        max(payments.payment_date) as payment_finalized_date,
        sum(payments.payment_amount) / 100.0 as total_amount_paid
    from payments
    where payments.payment_status <> 'fail'
    group by 1
)
, paid_orders as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        agg_payments.total_amount_paid,
        agg_payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join agg_payments on orders.order_id = agg_payments.order_id
    left join customers on orders.customer_id = customers.customer_id
)
, customer_orders as (
    select
        customers.customer_id,
        min(order_placed_at) as first_order_date,
        max(order_placed_at) as most_recent_order_date,
        count(order_id) as number_of_orders
    from customers
    left join orders on orders.customer_id = customers.customer_id
    group by 1
)

, customer_value as (
    select 
        paid_orders.order_id, 
        sum(t2.total_amount_paid) as total_value
    from paid_orders
    left join paid_orders t2 on paid_orders.customer_id = t2.customer_id and paid_orders.order_id >= t2.order_id
    group by 1
    order by paid_orders.order_id
)

select
    paid_orders.*,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
    case
        when customer_orders.first_order_date = paid_orders.order_placed_at then 'new' 
        else 'return'
    end as nvsr,
    customer_value.total_value as customer_lifetime_value,
    customer_orders.first_order_date as fdos
from paid_orders
left join customer_orders using (customer_id)
left outer join customer_value on customer_value.order_id = paid_orders.order_id
order by paid_orders.order_id
