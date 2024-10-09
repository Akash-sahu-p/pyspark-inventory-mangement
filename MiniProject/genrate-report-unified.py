from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import sum, col,avg,count,upper,round
import os


spark = SparkSession.builder \
    .appName("Reports Generation") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")


#
# print(os.getcwd())

output_dir = "output-reports"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)



# Load the Dimension and Fact tables
customers_dim = spark.read.csv("output/customers_dim.csv", header=True, inferSchema=True)
products_dim = spark.read.csv("output/products_dim.csv", header=True, inferSchema=True)
sellers_dim = spark.read.csv("output/sellers_dim.csv", header=True, inferSchema=True)
time_dim = spark.read.csv("output/time_dim.csv", header=True, inferSchema=True)
transaction_dim = spark.read.csv("output/transaction_dim.csv", header=True, inferSchema=True)
inventory_fact = spark.read.csv("output/inventory_fact.csv", header=True, inferSchema=True)


# just checking

customers_dim.show()
products_dim.show()
sellers_dim.show()
time_dim.show()
transaction_dim.show()
inventory_fact.show()




# transaction_dim.withColumn('sa', transaction_dim.PRODUCT_SELLING_PRICE).show()

# inventory_fact.withColumn('sa', inventory_fact.PRODUCT_SELLING_PRICE+2).show()






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

top_10_customers.toPandas().to_csv(f"{output_dir}/top_10_customers.csv")


# Report 2: State wise number of orders
statewise_orders = inventory_fact.join(customers_dim, "CUSTOMER_ID") \
    .groupBy(upper(col("CUSTOMER_STATE")).alias("CUSTOMER_STATE")) \
    .agg(count("TRANSACTION_ID").alias("ORDER_COUNT")) \
    .orderBy(col("CUSTOMER_STATE"),ascending = True)


statewise_orders.show()

statewise_orders.toPandas().to_csv(f"{output_dir}/statewise_orders.csv")



# Report 3: Preferred mode of payment across states
payment_mode_by_state = inventory_fact.join(transaction_dim, "TRANSACTION_ID") \
    .groupBy("TRANSACTION_TYPE") \
    .agg(count("TRANSACTION_ID").alias("ORDER_COUNT")) \
    .orderBy(col("ORDER_COUNT") , ascending = True)


payment_mode_by_state.show()

payment_mode_by_state.toPandas().to_csv(f"{output_dir}/payment_mode_by_state.csv")



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

quarterly_profit.toPandas().to_csv(f"{output_dir}/quarterly_profit.csv")



#updating the product table ( as we have multiple data for


# Report 5: Quarterly sales count of each product category

# filtering for required year


year_filter = int(input("enter the year"))

quarterly_sales_count = inventory_fact.join(time_dim, "TIME_DIM_ID") \
    .join(products_dim, "PRODUCT_ID") \
    .filter(col("YEAR") == year_filter)\
    .filter(col("CATEGORY_ID") == 4005) \
    .groupBy("YEAR", "QUARTER", "CATEGORY_ID", "PRODUCT Name") \
    .agg(count("TRANSACTION_ID").alias("SALES_COUNT")) \
    .orderBy("YEAR", "QUARTER", "CATEGORY_ID")


quarterly_sales_count.show()

quarterly_sales_count.toPandas().to_csv(f"{output_dir}/quarterly_sales_count.csv")



#stoping the spark session

spark.stop()