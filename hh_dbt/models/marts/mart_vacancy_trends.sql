{{
	config(
		materialized='table'
	)
}}

with vacancies as (
	select * from {{ ref('stg_vacancies') }}
),
final as (
	select
		published_date
		, count(*) as vacancy_count
		, count(case when salary_from > 0 then 1 end) as vacancies_with_salary
		, round(avg(case when salary_from > 0 then salary_avg end), 0) as avg_salary
		, string_agg(distinct vacancy_name, ', ' order by vacancy_name) as top_positions
	from vacancies
	group by published_date
)
select *
from final
order by published_date desc