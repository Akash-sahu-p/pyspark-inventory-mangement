
# code to create dimension table and fact table
# selecting required field to create dimension and fact table 


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, monotonically_increasing_id, row_number,min
import os
import pandas

file_path = 'input-data-set/Inventory_Management_Src1.csv'

# input_file_path = os.path.join(script_dir, relative_path)

# Initialize Spark session


spark = SparkSession.builder \
    .appName("Star Schema") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

output_dir = "output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def save_dataframe_as_csv(spark_df, filename):
    pandas_df = spark_df.toPandas()
    pandas_df.to_csv(f"{output_dir}/{filename}.csv", index=False)
    
#reading the main input file

try:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
except Exception as e:
    print("an error was occured reading the main input file ")
    print("ERROR: ", e)

# define output folder and handle the case when output does not exist


# creating  Customers Dimension Table

try:
    customers_df = df.select("CUSTOMER_ID", "Customer Name", "CUSTOMER_LOGIN_ID",
                             "CUSTOMER_STREET_ADDRESS", "CUSTOMER_CITY", "CUSTOMER_STATE",
                             "CUSTOMER_ZIP", "CUSTOMER_PHONE_NO").dropDuplicates()

    windowSpec = Window.orderBy(monotonically_increasing_id())
    customers_dim = customers_df.withColumn("CUSTOMER_DIM_ID", row_number().over(windowSpec))

    # show Customers Dimension table
    customers_dim.show()
except Exception as e:
    print("an error was occured while creating cutomers dimension table ")
    print("ERROR: ", e)

#saving the customers_dim file
try:
    save_dataframe_as_csv(customers_df, "customers_dim")
except Exception as e:
    print("an error was occured while writing(saving) cutomers dimension table ")
    print("ERROR: ", e)



# creating product dimension table

try:

    products_df = df.select("PRODUCT_ID", "CATEGORY_ID", "PRODUCT Name", "PRODUCT Brand",
                            "Product Model No", "PRODUCT_STOCK").dropDuplicates()

    products_df = df.groupby("PRODUCT_ID", "CATEGORY_ID", "PRODUCT Name", "PRODUCT Brand",
                             "Product Model No").agg(min(col("PRODUCT_STOCK")).alias("PRODUCT_STOCK"))

    products_dim = products_df.withColumn("PRODUCT_DIM_ID", row_number().over(windowSpec))

    # show  Products Dimension table
    products_dim.show()

except Exception as e:
    print("an error was occured while creating product dimension table ")
    print("ERROR: ", e)

# saving products_dim file
try:
    save_dataframe_as_csv(products_dim, "products_dim")

except Exception as e:
    print("an error was occured while saving(writing) product dimension table ")
    print("ERROR: ", e)


# Create Seller Dimension Table
try:
    sellers_df = df.select("SELLER_ID", "SELLER_NAME", "SELLER_RATING", "SELLER_STREET_ADDRESS",
                           "SELLER_CITY", "SELLER_STATE", "SELLER_ZIP", "SELLER_PHONE_NO").dropDuplicates()

    sellers_dim = sellers_df.withColumn("SELLER_DIM_ID", row_number().over(windowSpec))

    # show Seller Dimension table
    sellers_dim.show()  # write.csv("output/sellers_dim", header=True)

except Exception as e:
    print("an error was occured while creating seller dimension table ")
    print("ERROR: ", e)


# saving sellers_dim file
try:
    save_dataframe_as_csv(sellers_df, "sellers_dim")

except Exception as e:
    print("an error was occured while writing  seller dimension table ")
    print("ERROR: ", e)



# Create Time Dimension file

try:
    time_df = df.select("Date").dropDuplicates()

    # Extract Year, Quarter, Month, Day_of_Week
    from pyspark.sql.functions import to_date, year, quarter, month, dayofweek

    time_dim = time_df.withColumn("YEAR", year(to_date(col("Date"), "dd-MMM-yy"))) \
        .withColumn("QUARTER", quarter(to_date(col("Date"), "dd-MMM-yy"))) \
        .withColumn("MONTH", month(to_date(col("Date"), "dd-MMM-yy"))) \
        .withColumn("DAY_OF_WEEK", dayofweek(to_date(col("Date"), "dd-MMM-yy"))) \
        .withColumn("TIME_DIM_ID", row_number().over(windowSpec))

    # abc =  year(to_date("28-aug-11", "dd-mmm-yy"))
    # print(abc)

    time_dim.show() 

except Exception as e:
    print("an error was occured while creating Date dimension table ")
    print("ERROR: ", e)

# saving the time_dim table
try:
    save_dataframe_as_csv(time_dim,"time_dim")
except Exception as e:
    print("an error was occured while saving time dimension table " )
    print( "ERROR: " , e)


#  creatingTransaction  Table
try:

    transaction_df = df.select("TRANSACTION_ID", "TRANSACTION_DATE", "TRANSACTION_AMOUNT",
                               "TRANSACTION_TYPE", "DISPATCH_DATE", "EXPECTED_DATE", "DELIVERY_DATE").dropDuplicates()

    transaction_dim = transaction_df.withColumn("TRANSACTION_DIM_ID", row_number().over(windowSpec))

    transaction_dim.show()

except Exception as e:
    print("an error was occured while creating transaction dimension table ")
    print("ERROR: ", e)


# saving transaction dim file
try:
    save_dataframe_as_csv(transaction_df, "transaction_dim")

except Exception as e:
    print("an error was occured while writing  trasaction dimension table ")
    print("ERROR: ", e)


# creating Fact Table (Inventory)
try:
    inventory_fact_df = df.select("DATE", "TRANSACTION_ID", "PRODUCT_ID", "CUSTOMER_ID",
                                  "SELLER_ID", "PRODUCT_COST_PRICE", "PRODUCT_SELLING_PRICE").dropDuplicates()
    inventory_fact_df = inventory_fact_df.join(time_dim, "DATE", "inner")

    inventory_fact = inventory_fact_df.withColumn("FACT_ID", row_number().over(windowSpec))

    inventory_fact = inventory_fact.select("FACT_ID","TIME_DIM_ID","TRANSACTION_ID", "PRODUCT_ID", "CUSTOMER_ID",
                                  "SELLER_ID", "PRODUCT_COST_PRICE", "PRODUCT_SELLING_PRICE")

except Exception as e:
    print("an error was occured while creating inventory fact table ")
    print("ERROR: ", e)

# saving inventory_fact table
try:
    save_dataframe_as_csv(inventory_fact, "inventory_fact")

except Exception as e:
    print("an error was occured while saving invenotry fact table ")
    print("ERROR: ", e)


#stopping the spark session
spark.stop()
