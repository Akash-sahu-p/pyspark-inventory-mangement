#code to genrate required output reports


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import sum, col,avg,count,upper,round
import os


# starting the spark session
spark = SparkSession.builder \
    .appName("Reports Generation") \
    .getOrCreate()

# setting log level to error to reduce the logs
spark.sparkContext.setLogLevel("ERROR")

#defining output dir for saving the reports
output_dir = "output-reports"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


#custom function to read a csc file
def read_csv(path):
    temp_table = spark.read.csv(path, header=True, inferSchema=True)
    return temp_table
    

# Load the Dimension and Fact tables using the read_csv function
customers_dim = read_csv("output/customers_dim.csv")
products_dim = read_csv("output/products_dim.csv")
sellers_dim = read_csv("output/sellers_dim.csv")
time_dim = read_csv("output/time_dim.csv")
transaction_dim = read_csv("output/transaction_dim.csv")
inventory_fact = read_csv("output/inventory_fact.csv")


#custom function  to convert a df into pandas and save in csv
def save_dataframe_as_csv(spark_df, filename):
    pandas_df = spark_df.toPandas()
    pandas_df.to_csv(f"{output_dir}/{filename}.csv", index=False)

# printing DFs for debuging
# customers_dim.show()
# products_dim.show()
# sellers_dim.show()
# time_dim.show()
# transaction_dim.show()
# inventory_fact.show()

#building reports 

# Report 1: Transaction Amount Wise Top 10 Customers
top_10_customers = inventory_fact.join(customers_dim, "CUSTOMER_ID") \
    .join(time_dim, "TIME_DIM_ID") \
    .join(transaction_dim, "TRANSACTION_ID")\
    .where(col("YEAR") == 2011) \
    .groupBy("YEAR", "Customer Name",customers_dim.CUSTOMER_ID ) \
   .agg(sum("TRANSACTION_AMOUNT").alias("TOTAL_SELLING_PRICE")) \
    .orderBy("TOTAL_SELLING_PRICE",ascending = False) \
    .limit(10)

top_10_customers.show()
save_dataframe_as_csv(top_10_customers, "top_10_customers.csv")


# Report 2: State wise number of orders
statewise_orders = inventory_fact.join(customers_dim, "CUSTOMER_ID") \
    .groupBy(upper(col("CUSTOMER_STATE")).alias("CUSTOMER_STATE")) \
    .agg(count("TRANSACTION_ID").alias("ORDER_COUNT")) \
    .orderBy(col("CUSTOMER_STATE"),ascending = True)

statewise_orders.show()
save_dataframe_as_csv(statewise_orders, "statewise_orders.csv")



# Report 3: Preferred mode of payment across states
payment_mode_by_state = inventory_fact.join(transaction_dim, "TRANSACTION_ID") \
    .groupBy("TRANSACTION_TYPE") \
    .agg(count("TRANSACTION_ID").alias("ORDER_COUNT")) \
    .orderBy(col("ORDER_COUNT") , ascending = True)

payment_mode_by_state.show()
save_dataframe_as_csv(payment_mode_by_state, "payment_mode_by_state.csv")



# Report 4: Quarterly profit for a particular year
# Profit = PRODUCT_SELLING_PRICE - PRODUCT_COST_PRICE
year_filter = 2011  #  desired year
# we are using group by "YEAR" because we want year in our output

quarterly_profit = inventory_fact.join(time_dim, "TIME_DIM_ID") \
    .filter(col("YEAR") == year_filter) \
    .groupBy("YEAR", "QUARTER") \
    .agg(sum(col("PRODUCT_SELLING_PRICE") - col("PRODUCT_COST_PRICE")).cast("int").alias("PROFIT")) \
    .orderBy("QUARTER")

quarterly_profit.show()
save_dataframe_as_csv(quarterly_profit, "quarterly_profit.csv")


# Report 5: Quarterly sales count of each product category
# filtering for required year
year_filter = 2011

quarterly_sales_count = inventory_fact.join(time_dim, "TIME_DIM_ID") \
    .join(products_dim, "PRODUCT_ID") \
    .filter(col("YEAR") == year_filter)\
    .filter(col("CATEGORY_ID") == 4005) \
    .groupBy("YEAR", "QUARTER", "CATEGORY_ID", "PRODUCT Name") \
    .agg(count("TRANSACTION_ID").alias("SALES_COUNT")) \
    .orderBy("YEAR", "QUARTER", "CATEGORY_ID")

quarterly_sales_count.show()
save_dataframe_as_csv(quarterly_sales_count, "quarterly_sales_count.csv")



#stoping the spark session

spark.stop()
