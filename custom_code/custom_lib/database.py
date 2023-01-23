
import psycopg2

# TODO: use sqlalchemy + pandas
class PostgresConnection:
    def __init__(self) -> None:
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect("dbname=software_jobs user=airflow password=airflow host=127.0.0.1")
        print("Connected to DB")
        return self.conn

    def __exit__(self, *exc):
        self.conn.close()
        print("Closed connection")
