import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def test_connection():
	try:
		conn = psycopg2.connect(
			host = "localhost",
			port = os.getenv("POSTGRES_PORT"),
			database = os.getenv("POSTGRES_DB"),
			user = os.getenv("POSTGRES_USER"),
			password = os.getenv("POSTGRES_PASSWORD")
		)

		cursor = conn.cursor()

		cursor.execute("create schema if not exists raw;")
		cursor.execute("create schema if not exists staging;")
		cursor.execute("create schema if not exists marts;")

		conn.commit()

		cursor.execute("select current_database()")
		db_name = cursor.fetchone()

		print(f"Схемы созданы в базе {db_name}")

		cursor.close()
		conn.close()
	except Exception as e:
		print(f"Ошибка {e}")

if __name__ == "__main__":
	test_connection()