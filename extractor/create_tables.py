import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def create_tables():
	conn = psycopg2.connect(
		host = "localhost",
		port = os.getenv("POSTGRES_PORT"),
		database = os.getenv("POSTGRES_DB"),
		user = os.getenv("POSTGRES_USER"),
		password = os.getenv("POSTGRES_PASSWORD"))
	cursor = conn.cursor()

	cursor.execute("""
				create table if not exists raw.vacancies(
					id              VARCHAR(20) PRIMARY KEY,
					name            TEXT,
					published_at    TIMESTAMP,
					url             TEXT,
					salary_from     INTEGER,
					salary_to       INTEGER,
					salary_currency VARCHAR(10),
					employer_name   TEXT,
					requirement     TEXT,
					responsibility  TEXT,
					area_name       TEXT,
					loaded_at       TIMESTAMP DEFAULT NOW()
				);
				""")
	conn.commit()
	print("Таблицы созданы")
	cursor.close()
	conn.close()

if __name__ == "__main__":
	create_tables()