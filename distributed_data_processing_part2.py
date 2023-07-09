### Installing PySpark ###
!pip install pyspark
!pip install faker

from pyspark.sql import SparkSession as ss
from pyspark import SparkFiles
from faker import Faker
import json 
import pandas as pd
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, BooleanType

spark = (ss
         .builder
         .master('local')
         .appName('UE')
         .getOrCreate()
         )

"""# Example 6"""

"""
We will be working on US road works data:
https://www.kaggle.com/datasets/sobhanmoosavi/us-road-construction-and-closures Should be uploading by now (before the break we started). The data is large, so you can work with samples at the beginning, and when the solution is ready, calculate it on the full dataset:

Add a column to convert distance from miles to kilometers

Add a column where you enter how many days the road works lasted

Filter roadworks from 2020 and using cross
join with city data: https://tjach-ue.s3.eu-central-1.amazonaws.com/uscities.csv find the nearest city to each building
"""

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/uscities.csv")
spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/us_constructions.csv.gz")

cities = spark.read.csv("file://"+SparkFiles.get("uscities.csv"), header=True, inferSchema = True)
us_constr = spark.read.csv("file://"+SparkFiles.get("us_constructions.csv.gz"), header=True, inferSchema = True)

cities.show()
cities.printSchema()

us_constr.show()

us_constr_new = us_constr.withColumn("Distance(mi)", col('Distance(mi)').cast("integer"))

us_constr_new.printSchema()

us_constr_new = us_constr.withColumn("Distance(km)", format_number(col("Distance(mi)") * (161/100), 2))

us_constr_new.select("Distance(km)", "Distance(mi)").show()

us_constr_new.withColumn('Start_Time', to_date('Start_Time')).withColumn('End_Time', to_date('End_Time'))
us_constr_new.withColumn("Period", datediff('End_Time', 'Start_Time')).show()

#const_period = us_constr_new.select("Start_Time", "End_Time", "Period")

us_constr.withColumn("Start_Lat", col("Start_Lat").cast('integer'))
us_constr.withColumn("End_Lat", col("End_Lat").cast('integer'))
us_constr.withColumn("Start_Lng", col("Start_Lng").cast('integer'))
us_constr.withColumn("End_Lng",  col("End_Lng").cast('integer'))


cities.withColumn("lat", col('lat').cast("integer"))
cities.withColumn("lng", col('lng').cast("integer"))

us_constr_2020 = us_constr.withColumn('Start_Time', year('Start_Time'))
us_constr_2020 = us_constr.withColumn('End_Time', year('End_Time'))
us_c_2020 = us_constr_2020.where((col("Start_Time") == '2020') & (col("End_Time") == '2020'))


us_c_2020 = us_c_2020.select("Start_Time", "End_Time", "Start_Lat", "Start_Lng", "End_Lat", "End_Lng", "City", "State")
cities = cities.select("state_id", "city", "lat", "lng")

join = us_c_2020 = us_c_2020.join(cities, (cities.state_id == us_c_2020.State) & (cities.city == us_c_2020.City), "inner").show(truncate=False)

us_constr.withColumn("Start_Lat", col("Start_Lat").cast('integer'))
us_constr.withColumn("End_Lat", col("End_Lat").cast('integer'))
us_constr.withColumn("Start_Lng", col("Start_Lng").cast('integer'))
us_constr.withColumn("End_Lng",  col("End_Lng").cast('integer'))


cities.withColumn("lat", col('lat').cast("integer"))
cities.withColumn("lng", col('lng').cast("integer"))

us_constr_2020 = us_constr.withColumn('Start_Time', year('Start_Time'))
us_constr_2020 = us_constr.withColumn('End_Time', year('End_Time'))
us_c_2020 = us_constr_2020.where((col("Start_Time") == '2020') & (col("End_Time") == '2020'))


us_c_2020 = us_c_2020.select("Start_Time", "End_Time", "Start_Lat", "Start_Lng", "End_Lat", "End_Lng", "City", "State")
cities = cities.select("state_id", "city", "lat", "lng")

join = us_c_2020 = us_c_2020.join(cities, (cities.state_id == us_c_2020.State) & (cities.city == us_c_2020.City), "inner").show(truncate=False)


"""# Example 7"""
"""
By using the package
Faker: https://faker.readthedocs.io/en/master/ generate 10000 JSON files containing:

* Imię
* Nazwisko
* Datę Urodzenia
* Nr telefonu
* Miasto

Read these files with Spark and check:
Average name length
The month with the most people born
"""

fake = Faker()
for i in range(0,50):
  osoba = {}
  osoba['imie'] = fake.name()
  osoba['adres'] = fake.address()
  osoba['data'] = str(fake.date_of_birth())
  osoba['nr_telefonu'] = str(fake.phone_number())
  osoba['miasto'] = str(fake.city())
  print(osoba)
  with open(f'fake_data/data_{i}.json', 'w') as f:
    json.dump(osoba, f)

people = spark.read.json('fake_data')

people.show(truncate=False)
people.printSchema


people1 = people.withColumn("name_length", length("imie"))

people1 = people1.orderBy("name_length", ascending=False).show(truncate=False)

people2 = people.withColumn('data', month('data')).show()
people2.withColumn("data", cast("int"))
people2.groupBy("data").count().show()

"""# Example 8"""

"""
Using the data from the join exercise, convert the frame to Pandas
Using any visualization tool, draw the graphs:

* Population of each race in the US

* The number of people in each US state

*For those interested: Stacked bar chart for population in each state by race
"""

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/uscities.csv")

cities = spark.read.csv("file://"+SparkFiles.get("uscities.csv"), header=True, inferSchema = True)

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/states-by-race.csv")

state_race = spark.read.csv("file://"+SparkFiles.get("states-by-race.csv"), header=True, inferSchema = True)

cities.show()

state_race.show()

from pyspark.sql.functions import *

state_population = cities.groupBy("state_name").agg(sum("population").alias("state_population"))

state_population.show()

state_race_new = (
    state_race
        .withColumn("WhiteTotalPerc", col('WhiteTotalPerc').cast("integer"))
        .withColumn("BlackTotalPerc", col('BlackTotalPerc').cast("integer"))
        .withColumn("IndianTotalPerc", col('IndianTotalPerc').cast("integer"))
        .withColumn("AsianTotalPerc", col('AsianTotalPerc').cast("integer"))
        .withColumn("HawaiianTotalPerc", col('HawaiianTotalPerc').cast("integer"))
        .withColumn("OtherTotalPerc", col('OtherTotalPerc').cast("integer"))
)

state_race_new = (
    state_race
        .withColumn("WhiteTotalPerc", format_number(col("WhiteTotalPerc") * 100, 2))
        .withColumn("BlackTotalPerc", format_number(col("BlackTotalPerc") * 100, 2))
        .withColumn("IndianTotalPerc", format_number(col("IndianTotalPerc") * 100, 2))
        .withColumn("AsianTotalPerc", format_number(col("AsianTotalPerc") * 100, 2))
        .withColumn("HawaiianTotalPerc", format_number(col("HawaiianTotalPerc") * 100, 2))
        .withColumn("OtherTotalPerc", format_number(col("OtherTotalPerc") * 100, 2))
)


race_in_population = state_population.join(state_race_new,state_population.state_name == state_race_new.State,"inner").show(truncate=False)

state_population_pandas = state_population.toPandas()

print(state_population_pandas)

import matplotlib.pyplot as plt
import pandas as pd

data = state_population_pandas

seria = data["state_name"]
x = seria.tolist()
seria = data["state_population"]
y = seria.tolist()

#Chart

figure, axes = plt.subplots(figsize=(8, 6))
figure.tight_layout()

plt.xticks(rotation=90)

axes.bar(x, y, width = 0.5, edgecolor = 'orange', linewidth = 0.5)
axes.set(xlim = (-1, 52), 
         ylim = (0, 70000000),
         xlabel = 'Stan', ylabel = 'Population')
axes.set_title('Population in each US state')
plt.show()