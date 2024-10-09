from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, monotonically_increasing_id, row_number
import os
import pandas

file_path = 'input-data-set/Inventory_Management_Src1.csv'

# input_file_path = os.path.join(script_dir, relative_path)

# Initialize Spark session

spark = SparkSession.builder \
    .appName("Star Schema") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
df = spark.read.csv(file_path, header=True, inferSchema=True)

windowSpec = Window.orderBy(monotonically_increasing_id())
# Create Time Dimension Table
time_df = df.select("Date").dropDuplicates()

# Extract Year, Quarter, Month, Day_of_Week
from pyspark.sql.functions import year, quarter, month, dayofweek

time_dim = time_df.withColumn("YEAR", year(time_df.Date)) \
                  .withColumn("QUARTER", quarter(col("Date"))) \
                  .withColumn("MONTH", month(col("Date"))) \
                  .withColumn("DAY_OF_WEEK", dayofweek(col("Date"))) \
                  .withColumn("TIME_DIM_ID", row_number().over(windowSpec))


time_dim.select("TIME_DIM_ID", "YEAR", "QUARTER", "MONTH", "DAY_OF_WEEK", "Date").show()  #write.csv("output/time_dim", header=True)


spark.stop()


