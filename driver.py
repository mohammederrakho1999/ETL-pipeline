from pyspark.sql import SparkSession
from transform import Transform_data
from s3_utils import S3Module
import logging
import logging.config
import configparser


config = configparser.ConfigParser()
config.read_file(open("config.cfg"))



def create_sparksession():
    return SparkSession.builder.master("yarn").appName("etl").enableHiveSupport().getOrCreate()


def main():
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    TA = Transform_data(spark)

    # Modules in the project
    modules = {
        "books.csv": TA.transform_book_data,
        "user.csv" : TA.transform_user_data,
        "ratings.csv" : TA.transform_book_ratings_data,
    }

    logging.debug("\n\nCopying data from s3 landing zone to ...")
    S3M = S3Module()
    S3M.MoveData(source_bucket= config.get('BUCKET','LANDING_ZONE1999'), target_bucket= config.get('BUCKET', 'WORKING_ZONE1999'))

    files_in_working_zone = S3M.get_files(config.get('BUCKET', 'WORKING_ZONE1999'))

    # Cleanup processed zone if files available in working zone
    if len([set(modules.keys()) & set(files_in_working_zone)]) > 0:
        logging.info("Cleaning up processed zone.")
        S3M.clean_bucket(config.get('BUCKET', 'PROCESSED_ZONE1999'))

    for file in files_in_working_zone:
        if file in modules.keys():
            modules[file]()