{{
	config(
		materialized='table'
	)
}}

with vacancies as (
	select *
	from {{ ref('stg_vacancies') }}
	where salary_from > 0
),
skills_extacted as (
	select
		id
		, vacancy_name
		, salary_avg
		, requirement
		, case when lower(requirement) like '%python%' then 'Python' end as skill_1
		, case when lower(requirement) like '%sql%' then 'SQL' end as skill_2
		, case when lower(requirement) like '%airflow%' then 'Airflow' end as skill_3
		, case when lower(requirement) like '%dbt%' then 'dbt' end as skill_4
		, case when lower(requirement) like '%spark%' then 'Spark' end as skill_5
		, case when lower(requirement) like '%docker%' then 'Docker' end as skill_6
		, case when lower(requirement) like '%kafka%' then 'Kafka' end as skill_7
		, case when lower(requirement) like '%greenplum%' then 'Greenplum' end as skill_8
		, case when lower(requirement) like '%postgre%' then 'Postgre' end as skill_9
		, case when lower(requirement) like '%pandas%' then 'Pandas' end as skill_10
	from vacancies
),
--unfortunately not Oracle
skills_unpivot as (
	select id, vacancy_name, salary_avg, skill_1 as skill from skills_extacted where skill_1 is not null
	union all
	select id, vacancy_name, salary_avg, skill_2 as skill from skills_extacted where skill_2 is not null
	union all
	select id, vacancy_name, salary_avg, skill_3 as skill from skills_extacted where skill_3 is not null
	union all
	select id, vacancy_name, salary_avg, skill_4 as skill from skills_extacted where skill_4 is not null
	union all
	select id, vacancy_name, salary_avg, skill_5 as skill from skills_extacted where skill_5 is not null
	union all
	select id, vacancy_name, salary_avg, skill_6 as skill from skills_extacted where skill_6 is not null
	union all
	select id, vacancy_name, salary_avg, skill_7 as skill from skills_extacted where skill_7 is not null
	union all
	select id, vacancy_name, salary_avg, skill_8 as skill from skills_extacted where skill_8 is not null
	union all
	select id, vacancy_name, salary_avg, skill_9 as skill from skills_extacted where skill_9 is not null
	union all
	select id, vacancy_name, salary_avg, skill_10 as skill from skills_extacted where skill_10 is not null
),
final as (
	select
		skill
		, count(distinct id) as vacancy_count
		, round(avg(salary_avg), 0) as avg_salary
		, round(min(salary_avg), 0) as min_salary
		, round(max(salary_avg), 0) as max_salary
	from skills_unpivot
	group by skill
)
select *
from final
order by avg_salary desc