import boto3
import configparser
from pathlib import Path


config = configparser.ConfigParser()
config.read_file(open("config.cfg"))
#print(config.get("AWS","aws_access_key_id"))


class S3Module():
	"""docstring for ClassName"""
	def __init__(self):
		self.S3 = boto3.resource(service_name = "s3",region_name = "eu-west-1", aws_access_key_id = config.get("AWS","aws_access_key_id"), aws_secret_access_key = config.get("AWS","aws_secret_access_key ")) 
		self.LandingZone = config.get("BUCKET","LANDING_ZONE1999")
		self.WorkingZone = config.get("BUCKET","WORKING_ZONE1999")
		self.ProcessedZone = config.get("BUCKET","PROCESSED_ZONE1999")


	def move_data():

