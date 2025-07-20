-- models/marts/finance/fct_monthly_revenue.sql
{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['date_month'], 'type': 'btree'},
      {'columns': ['customer_id'], 'type': 'btree'}
    ],
    cluster_by=['date_month'],
    tags=['finance', 'monthly'],
    meta={
      'owner': 'finance-team',
      'contains_pii': false
    }
  )
}}

with orders as (
  select * from {{ ref('stg_orders') }}
),

customers as (
  select * from {{ ref('dim_customers') }}
),

order_items as (
  select * from {{ ref('stg_order_items') }}
),

-- Calculate monthly revenue aggregations
monthly_order_revenue as (
  select
    date_trunc('month', orders.order_date) as date_month,
    orders.customer_id,
    customers.customer_segment,
    customers.customer_region,
    
    count(distinct orders.order_id) as total_orders,
    sum(order_items.quantity * order_items.unit_price) as gross_revenue,
    sum(order_items.quantity * order_items.unit_price * orders.discount_rate) as total_discounts,
    sum(order_items.quantity * order_items.unit_price) - 
      sum(order_items.quantity * order_items.unit_price * orders.discount_rate) as net_revenue,
    
    avg(order_items.quantity * order_items.unit_price) as avg_order_value,
    
    -- Business logic for customer classification
    case 
      when sum(order_items.quantity * order_items.unit_price) >= 10000 then 'high_value'
      when sum(order_items.quantity * order_items.unit_price) >= 1000 then 'medium_value'
      else 'low_value'
    end as customer_value_tier

  from orders
  inner join order_items
    on orders.order_id = order_items.order_id
  inner join customers
    on orders.customer_id = customers.customer_id
  
  where orders.order_status = 'completed'
    and orders.order_date >= '2020-01-01'
    and orders.order_date < current_date
  
  group by 1, 2, 3, 4
),

-- Add period-over-period calculations
final as (
  select
    *,
    
    -- Previous month comparison
    lag(net_revenue, 1) over (
      partition by customer_id 
      order by date_month
    ) as prev_month_revenue,
    
    -- Year-over-year comparison
    lag(net_revenue, 12) over (
      partition by customer_id 
      order by date_month
    ) as prev_year_revenue,
    
    -- Calculate growth rates
    case 
      when lag(net_revenue, 1) over (partition by customer_id order by date_month) > 0
      then (net_revenue - lag(net_revenue, 1) over (partition by customer_id order by date_month)) / 
           lag(net_revenue, 1) over (partition by customer_id order by date_month) * 100
      else null
    end as month_over_month_growth_pct,
    
    -- Running totals
    sum(net_revenue) over (
      partition by customer_id 
      order by date_month 
      rows unbounded preceding
    ) as cumulative_revenue,
    
    -- Data quality flags
    case 
      when net_revenue < 0 then true
      else false
    end as has_negative_revenue_flag,
    
    -- Metadata
    current_timestamp as dbt_updated_at,
    '{{ var("etl_run_id") }}' as etl_run_id

  from monthly_order_revenue
)

select * from final
