#===============================================================================================================================================================#
# Purpose: This program will print a calendar for a given month and year.
# Creation Date: 2022-04-08
# Last Modified: 
# Language: PySpark 2.4.4
# Owner: Marcos Vinicios Martins Carnevale
#===============================================================================================================================================================#
# Importing libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# Setting the Spark Context
sc = SparkContext()
sqlc = SQLContext(sc)

# Initialize spark
spark = SparkSession.builder.appName("D_calendar").config("spark.some.config.option", "some-value").getOrCreate()

# Set range of dates
beginDate = '2000-01-01'
endDate = '2050-12-31'

# Create DataFrame with all the data
df = spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as CalendarDate")

# Create D_Calendar Table
df = df.withColumn("Year", year(col("calendarDate")))\
       .withColumn("Month", month(col("calendarDate")))\
       .withColumn("Day", dayofmonth(col("calendarDate")))\
       .withColumn("DayOfWeek", dayofweek(col("calendarDate")))\
       .withColumn("DayOfWeekName", date_format(col("calendarDate"), "EEEE"))\
       .withColumn("DayOfYear", dayofyear(col("calendarDate")))\
       .withColumn("QuarterOfYear", quarter(col("calendarDate")))

# Create Column "is_leap_year"
df = df.withColumn("IsLeapYear", when((col("Year") % 4 == 0)==True, 53).otherwise(52))
