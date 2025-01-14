with
employee_status as (
    select status_id, status_name
    from {{ ref('employee_status') }}
),

  last_customers as (
      select 
          employee_id, 
          customer_id,    
      from {{ source('dunder_mifflin', 'orders') }}
      qualify dense_rank() over(partition by employee_id order by order_date desc, order_id) <= 5
  )

  select
      last_customers.employee_id,
      employees.first_name || ' ' || employees.last_name as employee_full_name,
      last_customers.customer_id,
      customers.company_name,
      employee_status.status_name
  from last_customers
  left join {{ source('dunder_mifflin', 'employees') }} employees on employees.employee_id = last_customers.employee_id
  left join employee_status on employee_status.status_id = employees.employee_status_id
  left join {{ source('dunder_mifflin', 'customers') }} customers on customers.customer_id = last_customers.customer_id
  where employee_status.status_name in ('Suspended', 'Terminated', 'Retired')