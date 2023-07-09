### Installing PySpark ###
!pip install pyspark
!pip install faker

### Functions ###

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

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/uscities.csv")

cities = spark.read.csv("file://"+SparkFiles.get("uscities.csv"), header=True, inferSchema = True)

spark.sparkContext.addFile("https://tjach-ue.s3.eu-central-1.amazonaws.com/states-by-race.csv")

state_race = spark.read.csv("file://"+SparkFiles.get("states-by-race.csv"), header=True, inferSchema = True)

# cities.show()

# state_race.show()

from pyspark.sql.functions import *

state_population = cities.groupBy("state_name").agg(sum("population").alias("state_population"))

race_in_population = state_population.join(state_race,state_population.state_name == state_race.State,"inner").show(truncate=False)

# Population of each race in the US

race_in_USA = state_population.join(state_race,state_population.state_name == state_race.State,"inner")
race_in_USA.withColumn("Whitestate_populationTotalPerc", col('state_population').cast("integer"))

pandas_race_in_USA = race_in_USA.toPandas()

# print(pandas_race_in_USA)

data2 = pandas_race_in_USA

data2["White"] = data2[["state_population", "WhiteTotalPerc"]].prod(axis=1)

data2["Black"] = data2[["state_population", "BlackTotalPerc"]].prod(axis=1)

data2["Indian"] = data2[["state_population", "IndianTotalPerc"]].prod(axis=1)

data2["Asian"] = data2[["state_population", "AsianTotalPerc"]].prod(axis=1)

data2["Hawaiian"] = data2[["state_population", "HawaiianTotalPerc"]].prod(axis=1)

data2["Other"] = data2[["state_population", "OtherTotalPerc"]].prod(axis=1)

del data2["WhiteTotalPerc"]

del data2["BlackTotalPerc"]

del data2["IndianTotalPerc"]

del data2["AsianTotalPerc"]

del data2["HawaiianTotalPerc"]

del data2["OtherTotalPerc"]

# print(data2)

data2_sum = data2[["White", "Black","Indian", "Asian", "Hawaiian", "Other"]].sum()
print(data2_sum)

import matplotlib.pyplot as plt
import pandas as pd

x = ["White", "Black","Indian", "Asian", "Hawaiian", "Others"]
y = [2.884318e+08, 5.105085e+07, 3.272190e+06, 2.417124e+07, 8.093490e+05, 3.638332e+07]

figure, axes = plt.subplots()
figure.tight_layout()

#plt.xticks(rotation=90)

axes.bar(x, y, width = 0.8, edgecolor = 'black', linewidth = 2)
"""
axes.set(xlim = (-1, 6), xticks = np.arange(0,5),
         ylim = (0, 3.0e+08),
         xlabel = 'Rasa', ylabel = 'Populacja')
"""
# po zastosowaniu powyższego znika podpis "Others"

axes.set_title('The U.S. population is racially diverse')
plt.show()