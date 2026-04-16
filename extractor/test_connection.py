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

		cursor.execute("select version();")
		version = cursor.fetchone()
		print(f"Успех, версия PG {version[0]}")

		cursor.close()
		conn.close()
	except Exception as e:
		print(f"Ошибка {e}")

if __name__ == "__main__":
	test_connection()