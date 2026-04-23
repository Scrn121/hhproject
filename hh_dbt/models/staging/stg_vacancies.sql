{{
  config(
    materialized='table',
    schema='staging'
  )
}}

with source as (
	select * from {{ source('raw', 'vacancies') }}
),

cleaned as (
	select
		id
		, name as vacancy_name
		, published_at
		, url
		, coalesce(salary_from, 0) as salary_from
		, coalesce(salary_to, 0) as salary_to
		, coalesce(salary_currency, 'RUR') as salary_currency
		, employer_name
		, requirement
		, responsibility
		, area_name
		, loaded_at
		, date(published_at) as published_date
		, case
			when salary_to > 0 then (salary_from + salary_to) / 2
			when salary_from > 0 then salary_from
			else 0
		end as salary_avg
	from source
)

select * from cleaned