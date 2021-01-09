from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import configparser
import logging


config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

class OlistTransform:
	"""This class performs transformations operations 
	   on the diffrent datasets
	"""

	def __init__(self, spark):
		self._spark = spark
		self._load_path = "s3a" + config.get("BUCKET","WORKING_ZONE")
		self._save_path = "s3a" + config.get("BUCKET","PROCESSED_ZONE")

	def transform_book_data(self):
		books = self._spark.read.format("csv").option("header","true") \
		                                      .option("sep",";") \
		                                      .option("inferShema","true") \
		                                      .load("BX-Books.csv")

		books = books.where(col("Year-Of-publication") != 0) \
		             .withColumn("Year-Of-publication", books["Year-Of-publication"].cast(IntegerType()))

        books = books.filter(col("Year-Of-publication") < 2006)
        books = books.withColumn("Publisher", fn.regexp_replace("Publisher","N/A","other"))

        books.repartition(2).write \
                        .csv(path = self._save_path + "/books/", mode = "overwrite", compression = "gzip", header = True)
                        
    def transform_data(self):
		df_customers = self._spark.read.format("csv") \
		                               .option("header","true") \
		                               .option("inferShema","true") \
		                               .load(self._load_path + "/olist_customers_dataset.csv/")

        df_order = self._spark.read.format("csv") \
		                           .option("header","true") \
		                           .option("inferShema","true") \
		                           .load(self._load_path + "/olist_orders_dataset.csv/")

		df_product = self._spark.read.format("csv") \
		                             .option("header","true") \
		                             .option("inferShema","true") \
		                             .load(self._load_path + "/olist_products_dataset.csv/")

		df_order_items = self._spark.read.format("csv") \
		                             .option("header","true") \
		                             .option("inferShema","true") \
		                             .load(self._load_path + "/olist_order_items_dataset.csv/")

		logging.debug("begin transformation")

		df_customer_data = df_customers.filter(df_customers.customer_id != "NaN") \
		                               .select(["customer_id","customer_id","customer_state"])

		df_order_data = df_order.join(df_order_items, on = ["order_id"], how = "inner") \
		                        .filter(df_order.customer_id !="NaN") \
		                        .select(["customer_id","order_id","order_status","order_delivered_customer_date","product_id"])

        df_order_data.persist()
        fn.broadcast(df_order_data)

        df_product_data = df_order_data.join(df_product, on=["product_id"], how = "inner") \
                                       .filter(df_product.product_category_name !="NaN") \
                                       .select(["product_id","product_category_name","product_name_lenght","product_description_lenght"])


        logging.debug("repartitiong data and saving it")

        df_customer_data.repartition(2).write \
                        .csv(path = self._save_path + "/customers/", mode = "overwrite", compression = "gzip", header = True)
        df_order_data.repartition(2).write \
                        .csv(path = self._save_path + "/order/", mode = "overwrite", compression = "gzip", header = True)
        df_product_data.repartition(2).write \
                        .csv(path = self._save_path + "/products/", mode = "overwrite", compression = "gzip", header = True)






