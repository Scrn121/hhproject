# As far as I couldn't have get a real API. I've made up my mind to get my own generator of vacancies

import random
import uuid
from datetime import datetime, timedelta

companies = [
	"Яндекс", "Сбер", "Тинькофф", "Авито", "VK", "Озон",
	"Касперский", "2ГИС", "Lamoda", "Wildberries", "МТС", "Билайн",
	"Ростелеком", "Газпром Нефть", "Альфа-Банк", "Райффайзен"
]

cities = [
	"Москва", "Санкт-Петербург", "Екатеринбург",
	"Новосибирск", "Казань", "Нижний Новгород"
]

vacancy_types = {
	"Data Engineer": ["Python", "Airflow", "Spark", "dbt", "Kafka", "PostgreSQL"],
	"Analytics Engineer": ["dbt", "SQL", "Python", "Looker", "BigQuery", "Redshift"],
	"Data Analyst": ["SQL", "Python", "Tableau", "Excel", "pandas", "Power BI"],
	"Python Developer": ["Python", "FastAPI", "Django", "PostgreSQL", "Redis", "Docker"],
	"Data Scientist": ["Python", "pandas", "sklearn", "TensorFlow", "SQL", "Jupyter"],
	"ML Engineer": ["Python", "PyTorch", "Docker", "Kubernetes", "MLflow", "Airflow"],
	"Backend Developer": ["Python", "Go", "PostgreSQL", "Redis", "Kafka", "Docker"],
	"DWH Developer": ["SQL", "dbt", "Greenplum", "Airflow", "Python", "Data Vault"],
}

salary_ranges = {
	"Data Engineer":      (150, 300),
	"Analytics Engineer": (140, 280),
	"Data Analyst":       (100, 220),
	"Python Developer":   (130, 280),
	"Data Scientist":     (140, 300),
	"ML Engineer":        (160, 350),
	"Backend Developer":  (130, 270),
	"DWH Developer":      (130, 260),
}

requirements_templates = [
	"Опыт работы от {n} лет. Знание {skills}.",
	"Уверенное владение {skills}. Опыт от {n} лет.",
	"Требуется опыт с {skills} не менее {n} лет.",
	"Знание {skills}. Опыт работы в команде от {n} лет.",
]

resposibilities_templates = [
	"Разработка и поддержка ETL-пайплайнов.",
	"Построение аналитических витрин данных.",
	"Оптимизация SQL-запросов и работа с большими данными.",
	"Разработка микросервисов и REST API.",
	"Построение ML-моделей и их внедрение в продакшн.",
	"Поддержка и развитие Data Platform компании.",
	"Проектирование архитектуры хранилища данных.",
]

def generate_vacancy() -> dict:

	vacancy_name = random.choice(list(vacancy_types.keys()))
	skills_pool = vacancy_types[vacancy_name]

	selected_skills = random.sample(skills_pool, k = random.randint(2,4))
	skills_str = ", ".join(selected_skills)

	#salary block
	salary = None
	if random.random() > 0.3:
		min_sal, max_sal = salary_ranges[vacancy_name]

		# min salary exists if salary does
		salary_from = random.randint(min_sal, max_sal - 30) * 1000

		# exists in 70% of cases
		salary_to = None
		if random.random() > 0.3:
			salary_to = salary_from + random.randint(20, 80) * 1000

		salary = {
			"from": salary_from,
			"to": salary_to,
			"currency": "RUR"
		}

	# The date when the vacancy was published
	days_ago = random.randint(0, 30)
	hours_ago = random.randint(0,23)
	published_at = datetime.now() - timedelta(days=days_ago, hours=hours_ago)

	#requirements
	requirement = random.choice(requirements_templates).format(
		n = random.randint(1, 5),
		skills = skills_str
	)

	# getting to hh API format
	vacancy_id = str(uuid.uuid4())[:8]

	return {
		"id": vacancy_id,
		"name": vacancy_name,
		"published_at": published_at.isoformat(),
		"alternate_url": f"https://hh.ru/vacancy/{vacancy_id}",
		"salary": salary,
		"employer": {
			"name": random.choice(companies)
		},
		"snippet": {
			"requirement": requirement,
			"responsibility": random.choice(resposibilities_templates)
		},
		"area": {
			"name": random.choice(cities)
		}
	}
# Generate 20 vacancies
def generate_vacancies(n: int = 20) -> list:
	return [generate_vacancy() for _ in range(n)]

# The direct run gives just 1 vacancy
if __name__ == "__main__":
	import json
	vacancy = generate_vacancy()
	print(json.dumps(vacancy, ensure_ascii=False, indent=2))