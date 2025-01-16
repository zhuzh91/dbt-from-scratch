with source as (
    select * from {{ source('dunder_mifflin', 'shippers') }}
),

renamed as (
    select
        shipper_id,
        company_name,
        phone
    from source
)

select * from renamed
