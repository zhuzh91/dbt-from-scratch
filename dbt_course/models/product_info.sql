with 

orders as (
    select
        product_id,
        count(order_id) as times_ordered,
        sum(line_total) as gross_sales
    from {{ source('dunder_mifflin', 'order_details') }}
    group by all
),
categories as (
    select
        category_id,
        category_name
    from {{ source('dunder_mifflin', 'categories') }}
),
suppliers as (
    select
        supplier_id,
        company_name
    from {{ source('dunder_mifflin', 'suppliers') }}
)
select
    products.product_id,
    products.product_name,
    categories.category_name,
    suppliers.company_name as supplier_name,
    products.units_in_stock,
    products.units_on_order,
    products.discontinued,
    orders.times_ordered,
    orders.gross_sales
from {{ source('dunder_mifflin', 'products') }} as products
left join orders on orders.product_id = products.product_id
left join categories on categories.category_id = products.category_id
left join suppliers on suppliers.supplier_id = products.supplier_id
order by orders.gross_sales desc