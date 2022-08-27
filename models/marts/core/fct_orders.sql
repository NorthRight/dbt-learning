with orders as  (
    select  
      order_id,
      customer_id,
      order_date,
      status
    from {{ ref('stg_orders' )}}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_payments as (
    select
        order_id,
        sum(case when payment_state = 'success' then amount end) as amount
    from payments
    group by 1
),

final as (

    select
        ot.order_id,
        ot.customer_id,
        ot.order_date,
        coalesce(op.amount, 0) as amount

    from orders as ot
    left join order_payments as op 
    on ot.order_id = op.order_id
)

select * from final