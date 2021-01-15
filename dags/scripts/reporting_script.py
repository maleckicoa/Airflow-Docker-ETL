import numerizer

import pandas as pd
import psycopg2
from sqlalchemy import create_engine


conn = psycopg2.connect(
    host="172.17.0.1",
    database="postgres",
    user="postgres",
    password="postgres",
    port="5432")

cur = conn.cursor()

cur.execute("SET SCHEMA 'reporting'")
cur.execute("DROP TABLE IF EXISTS phonebook ")
conn.commit()

df  = pd.read_sql("SELECT * FROM staging.phonebook;", conn);


table = "phonebook"
engine = create_engine('postgresql+psycopg2://postgres:postgres@172.17.0.1:5432/postgres')
df.to_sql(table, engine, schema='reporting')


conn.close()
