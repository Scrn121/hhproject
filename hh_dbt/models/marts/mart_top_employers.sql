--Top open vacancies
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
		employer_name
		, count(*) as vacancy_count
		, count(case when salary_from > 0 then 1 end) as vacancies_with_salary
		, round(avg(case when salary_from > 0 then salary_avg end), 0) as avg_salary_offered
	from vacancies
	where employer_name is not null
	group by employer_name
)
select *
from final
order by vacancy_count desc
limit 20