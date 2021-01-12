from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import configparser
import logging


config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

class Transform_data:
	"""This class performs transformations operations on the diffrent datasets"""

    def __init__(self, spark):
	self._spark = spark
	self._load_path = "s3a" + config.get("BUCKET","WORKING_ZONE")
	self._save_path = "s3a" + config.get("BUCKET","PROCESSED_ZONE")

    def transform_book_data(self):
	logging.debug("inside transform books data")
	books = self._spark.read.format("csv").option("header","true") \
		                                  .option("sep",";") \
		                                  .option("inferShema","true") \
		                                  .load(self._load_path + "/BX-Books.csv/")

	books = books.where(col("Year-Of-publication") != 0) \
		         .withColumn("Year-Of-publication", books["Year-Of-publication"].cast(IntegerType()))

        books = books.filter(col("Year-Of-publication") < 2006)
        books = books.withColumn("Publisher", fn.regexp_replace("Publisher","N/A","other"))
        
	logging.debug("writing books data")
        books.write \
             .csv(path = self._save_path + "/books/", mode = "overwrite", compression = "gzip", header = True)
	
                        
    def transform_user_data(self):
	logging.debug("inside transform user data")
	Users = self._spark.read.format("csv") \
		                               .option("header","true") \
	                                       .option("sep",";") \
		                               .option("inferShema","true") \
		                               .load(self._load_path + "/Users.csv/")
	
	
	Users = Users.withColumn("Age", Users["Age"].cast(IntegerType()))
	Users = Users.withColumn("Age", fn.when(((Users.Age > 90) | (Users.Age < 5)) , fn.lit("null")).otherwise(Users.Age))
	Split_col = fn.split(Users["Location"], ",")
	Users = Users.withColumn("City", split_col.getItem(0))
        Users = Users.withColumn("State", split_col.getItem(1))
        Users = Users.withColumn("country", split_col.getItem(2))
	Users = Users.drop("Location")
	
	logging.debug("writing User data")
	Users.repartition(2).write \
                                .csv(path = self._save_path + "/Users/", mode = "overwrite", compression = "gzip", header = True)

    def transform_book_ratings_data(self):
	logging.debug("inside trasform book ratings data")
	
	book_ratings = self._spark.read.format("csv") \ 
	                    .option("header","true") \
	                    .option("sep",";")\ 
	                    .option("inferShema","true") \
	                    .load(self._load_path, + ".csv")
	
	books = self._spark.read.format("csv") \ 
	                    .option("header","true") \
	                    .option("sep",";")\ 
	                    .option("inferShema","true") \
	                    .load(self._load_path, + "/books.csv/")
	
	
	book_ratings  = book_ratings.where(book_ratings['ISBN'].isin(books.select('ISBN').distinct().rdd.flatMap(lambda x:x).collect()[0]))
	book_ratings = book_ratings.select(["ISBN","User-ID","Book-Rating"])
	logging.debug("writing the data")
	book_ratings.repartition(2).write \
                                   .csv(path = self._save_path + "/book_ratings/", mode = "overwrite", compression = "gzip", header = True)
	
