from sqlalchemy import create_engine, MetaData, Integer, String, Table, Column, Float
from datetime import date
import csv

engine = create_engine('sqlite:///database.db', echo=True)

meta = MetaData()

def load_items_csv(csvfile):
    records = []
    with open(csvfile, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            records.append(row)
    return records

measure_records = load_items_csv("clean_measure.csv")
stations_records = load_items_csv("clean_stations.csv")

for m in measure_records:
    m["date"] = date.fromisoformat(m["date"])

print(stations_records, "\n")
print(measure_records, "\n")
print(engine)

measure = Table(
    "measure", meta,
    Column("station", String),
    Column("date", date),
    Column("precip", Float),
    Column("tobs", Integer)
)

stations = Table(
    "stations", meta,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String)
)

measure_records_insert = measure.insert().values(measure_records)
stations_records_insert = stations.insert().values(stations_records)

conn = engine.connect()
conn.execute(measure_records_insert)
conn.execute(stations_records_insert)

conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
conn.execute("SELECT * FROM measure LIMIT 5").fetchall()