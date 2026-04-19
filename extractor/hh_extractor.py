import os
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def get_vacancies() -> list:
	mock_path = os.path.join(os.path.dirname(__file__), "mock_vacancies.json")

	with open (mock_path, "r", encoding="UTF-8") as f:
		return json.load(f)

def parse_vacancy(vacancy:dict) -> dict:
	# Так как зп может быть не указана
	salary = vacancy.get("salary") or {}

	return {
		"id": vacancy.get("id"),
		"name": vacancy.get("name"),
		"published_at": vacancy.get("published_at"),
		"url": vacancy.get("alternate_url"),
		"salary_from": salary.get("from"),
		"salary_to": salary.get("to"),
		"salary_currency": salary.get("currency"),
		"employer_name": vacancy.get("employer", {}).get("name"),
		"requirement": vacancy.get("snippet", {}).get("requirement"),
		"responsibility": vacancy.get("snippet", {}).get("responsibility"),
		"area_name": vacancy.get("area", {}).get("name"),
	}

def get_db_connection():

	return psycopg2.connect(
	host="localhost",
	port=os.getenv("POSTGRES_PORT"),
	database=os.getenv("POSTGRES_DB"),
	user=os.getenv("POSTGRES_USER"),
	password=os.getenv("POSTGRES_PASSWORD")
	)

def save_vacancies(vacancies: list, conn) -> int:

	cursor = conn.cursor()
	saved = 0

	for vacancy in vacancies:
		cursor.execute("""
				 insert into raw.vacancies (id, name, published_at, url,salary_from, salary_to, salary_currency,employer_name, requirement, responsibility, area_name)
				 values( %(id)s, %(name)s, %(published_at)s, %(url)s,%(salary_from)s, %(salary_to)s, %(salary_currency)s,%(employer_name)s, %(requirement)s,%(responsibility)s, %(area_name)s)
				 on conflict (id) do nothing;""", vacancy)
		saved += cursor.rowcount

	conn.commit()
	cursor.close()
	return saved

def run():
	print(f"[{datetime.now()}] начало загрузки")

	raw_vacancies = get_vacancies()

	print(f"Прочитано из файла: [{len(raw_vacancies)}] вакансий")

	parsed = [parse_vacancy(v) for v in raw_vacancies]

	conn = get_db_connection()
	saved = save_vacancies(parsed, conn)
	conn.close()

	print(f"Новых записей: {saved}")
	print(f"[{datetime.now()}] готово")


if __name__ == "__main__":
	run()