import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd

url = "dbname=chinook host=localhost port=5435 user=postgres"
conn = psycopg2.connect(url)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
cursor = conn.cursor()

cursor.execute("CREATE DATABASE bike_store")
conn.commit()

url = "dbname=bike_store host=localhost port=5435 user=postgres"
conn = psycopg2.connect(url)
cursor = conn.cursor()

category = '''
            CREATE TABLE IF NOT EXISTS categories(
            category_id integer PRIMARY KEY,
            category_name varchar(50))
           '''
cursor.execute(category)
conn.commit()

print(pd.read_sql("SELECT * FROM categories", conn))
conn.close()




