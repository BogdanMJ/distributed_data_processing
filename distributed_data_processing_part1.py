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

"""# Example 1"""

### Adding files from the Internet without downloading them to disk###

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/uscities.csv")
spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/us_constructions.csv.gz")

cities = spark.read.csv("file://"+SparkFiles.get("uscities.csv"), header=True, inferSchema = True)

cities.show()

### Financial fraud ###

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/fraudTrain.csv.gz")

fraud = spark.read.csv("file://"+SparkFiles.get("fraudTrain.csv.gz"), header=True, inferSchema = True)

fraud.show()

fraud.printSchema()

### Stage 1 ###

cw1 = fraud.select('amt', 'city_pop','gender')

cw1.show()

cw1.groupBy('gender').agg(
    sf.mean('city_pop').alias('srednia'),
    sf.min('city_pop').alias('minimum'),
    sf.max('city_pop').alias('maksimum'),
).show()

cw1.groupBy('gender').agg(
    sf.mean('amt').alias('srednia'),
    sf.min('amt').alias('minimum'),
    sf.max('amt').alias('maksimum'),
).show()

### Stage 2 ###

cw2 = fraud.select('amt', 'gender').show()

fraud.groupBy("gender").count().show()

fraud.groupBy('gender').agg(sf.mean('amt').alias('srednia')).show()

### Stage 3 ###

fraud.groupBy("category").count().show()
fraud.groupBy('category').agg(sf.mean('amt').alias('srednia')).show()

### Stage 4 ###

fraud4 = fraud[fraud['gender'] == 'F']
fraud4.groupBy("city").count().orderBy('count', ascending=False).show()

### Stage 5 ###

fraud5 = fraud[fraud['gender'] == 'M']

fraud5.groupBy("state").count().orderBy('count').show()

### Stage 6 ###
"""
Check whether the occupation affects the deviation of the transaction amount from the average
"""

fraud.select('amt', 'job').show()

fraud.select("amt").summary("count", "min", "mean", "max").show()

fraud.groupBy('job').agg(sf.mean('amt').alias('srednia')).show()

### Stage 7 ###

"""
Assuming that the data is from 2019-2020, check if any minors (<21 years old) performed transactions. How many were there?
"""

from pyspark.sql.functions import *

cw7 = fraud.select('trans_date_trans_time', 'dob')
cw7 = cw7.withColumn('transaction_year', year('trans_date_trans_time'))
cw7 = cw7.withColumn('birth_year', year('dob'))

cw7_new = cw7.select('transaction_year', 'birth_year')
cw7_new = cw7_new.withColumn("diff", col("transaction_year") - col("birth_year"))

df = cw7_new.where(col("diff") < 21)
df.show()
df.summary().show()

### Stage 9 ###

"""
How many months have elapsed between the first and last crime?
"""

first_date = fraud.select("_c0", "trans_date_trans_time").show(1)
last_date = fraud.select("_c0", "trans_date_trans_time").orderBy(desc("_c0")).show(1)


df = spark.createDataFrame([('2019-01-01 00:00:18', '2020-06-21 12:13:37')], ['date1', 'date2'])
df.select(months_between(df.date1, df.date2).alias('months')).collect()

df = spark.createDataFrame([('1997-02-28 10:30:00', '1996-10-30')], ['date1', 'date2'])
df.select(months_between(df.date1, df.date2).alias('months')).collect()

"""# Example 2"""

"""
*Using a file
https://tjach-ue.s3.eu-central-1.amazonaws.com/states0-byrace.csv and data on cities (https://tjach-ue.s3.eu-central1.amazonaws.com/uscities. csv) join these two dataframes with a join

* Check that all of the states by race lines are reflected in uscities (use the appropriate join

* Check that all of the uscities lines are reflected in the states by race (use the appropriate join

* Calculate the population of each race based on percentages and display a frame of the number of people by race in each state
"""

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/uscities.csv")

cities = spark.read.csv("file://"+SparkFiles.get("uscities.csv"), header=True, inferSchema = True)

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/states-by-race.csv")

state_race = spark.read.csv("file://"+SparkFiles.get("states-by-race.csv"), header=True, inferSchema = True)

cities.show()

state_race.show()

# join these two dataframes with a join

joined1 = cities.join(state_race,cities.state_name == state_race.State,"inner").show(truncate=False)

#Check if all of the states by race lines are reflected in uscities (use the appropriate join)

join2 = state_race.join(cities, state_race.State == cities.state_name,"leftanti").show(truncate=False)

#Check if all uscities lines are reflected in states by race (use appropriate join)
join3 = cities.join(state_race, cities.state_name == state_race.State,"leftanti").show(truncate=False)

# Calculate the population of each race based on percentages and display a frame of the number of people by race in each state
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

"""# Example 3"""


"""
They use fraudTrain data and window functions,
answer the questions:

1. Determine the average, min and max transaction amounts in each state in each year
2. List the top three fraudulent transactions in each category
3. For those interested: for each credit card, determine if more times the value
transactions increased or decreased (i.e. compare in pairs sorted by
trades and see if there were more "rising" or "falling" pairs)
"""

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/fraudTrain.csv.gz")

fraud = spark.read.csv("file://"+SparkFiles.get("fraudTrain.csv.gz"), header=True, inferSchema = True)

fraud.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, year, avg, min, max


fraud = fraud.withColumn('year', year('trans_date_trans_time'))

fraud_year = fraud.groupBy('state', 'year').agg(avg('amt').alias('avg_amt'), min('amt').alias('min_amt'), max('amt').alias('max_amt'))

fraud_year.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, year, avg, min, max, col, desc

fraud_new = fraud.select("category", "amt")

windowSpec = Window.partitionBy("category").orderBy(desc("amt"))

fraud_new.withColumn("row", row_number().over(windowSpec)).filter(col("row") <= 3).show()

"""# Example 4"""


"""
* Take the data from the Faker exercise
* Save them to csv files partitioned around the city.
* Take one of the selected cities and save the date of birth data to python
     letters. Sort this list and print it to the screen
"""

fake = Faker()
for i in range(0,50):
  osoba = {}
  osoba['imie'] = fake.name()
  osoba['miasto'] =fake.city()
  osoba['data'] = str(fake.date_of_birth())
  print(osoba)
  with open(f'fake_json/data_{i}.json', 'w') as f:
    json.dump(osoba, f)

df_fake = spark.read.json('fake_json')
df_fake.show()

from pyspark.sql.functions import *
import pandas
df_fake.show(truncate=False)

df_fake.write.partitionBy("miasto").csv("fake_csv2")

miasta = spark.read.csv("fake_csv2")

miasta = miasta.withColumnRenamed("_c0","data")
miasta = miasta.withColumnRenamed("_c1","imie")
miasta = miasta.withColumnRenamed("_c2","miasto")

miasta.show()

miasta = miasta[miasta['miasto'] == 'South Pamela']

miasta = miasta.select("data")

miasta.show()

miasta_new = miasta.collect()

print(miasta_new)

"""# Example 5"""

"""
* Take data with 50,000 artificially generated identities (https://tjachue.s3.eu-central-1.amazonaws.com/50k.csv)

* Manually read several credit/debit card numbers and
   PESEL numbers ("NationalID")

* In "regular" Python, implement functions to validate credit card numbers and PESEL numbers.

* Test these functions and then use them as UDFs for a large data frame by adding the column " validPESEL " and validCC

* For those interested: rework the functions to return additional information (e.g. date
birth and gender for PESEL or additional credit card information such
like here: https://www.validcreditcardnumber.com/com/), return them as type
array in Spark, then split the returned array type into single ones
columns.

"""

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/50k.csv")
losowi = spark.read.csv("file://"+SparkFiles.get("50k.csv"), header=True, inferSchema = True)

losowi.show()

pkt1 = losowi.select('CCNumber', 'NationalID')
pkt1.printSchema()
pkt1.show(25)

pkt1_new = pkt1.withColumn("NationalID", sf.col('NationalID').cast("string"))
pkt1_new.printSchema()
pkt1_new.show(25)

from pyspark.sql.types import ArrayType, BooleanType

@udf(returnType=BooleanType())
def check_pesel(p: str):
    if len(p) != 11:
        return False
    l=int(p[10])
    suma =((l*int(p[0]))+(3*int(p[l]))+(7*int(p[2]))+(9*int(p[3]))+((l*int(p[4])))+(3*int(p[5]))+(7*int(p[6]))+(9*int(p[7]))+
        (l*int(p[8]))+(3*int(p[9])))
    kontrola=10-(suma %10)
    if (kontrola == 10) or (l==kontrola):
        return True
    else:
        return False


pesel_data_array = [{"PESEL": '93032703296'},{"PESEL": '11111111111'}]
df_pesel = spark.createDataFrame(pesel_data_array)

df_pesel = df_pesel.withColumn("Pesel_OK", check_pesel(sf.col('PESEL')))
df_pesel.show()


pkt2 = losowi.select('CCNumber', 'NationalID')
pkt2 = pkt2.withColumn("NationalID", sf.col('NationalID').cast("string"))
pkt2 = pkt2.withColumn("Pesel_OK", check_pesel(sf.col("NationalID")))
pkt2.show()