import boto3
import configparser
from pathlib import Path


config = configparser.ConfigParser()
config.read_file(open("config.cfg"))
#print(config.get("AWS","aws_access_key_id"))


class S3Module():
	"""docstring for ClassName"""
	def __init__(self):

		self.S3 = boto3.resource(service_name = "s3",region_name = "eu-west-1", 
			aws_access_key_id = config.get("AWS","aws_access_key_id"), 
			aws_secret_access_key = config.get("AWS","aws_secret_access_key")) 

		self.LandingZone = config.get("BUCKET","LANDING_ZONE1999")
		self.WorkingZone = config.get("BUCKET","WORKING_ZONE1999")
		self.ProcessedZone = config.get("BUCKET","PROCESSED_ZONE1999")


	def MoveData(self, source_bucket, target_bucket):


		if source_bucket == None:
			source_bucket = self.LandingZone
		if target_bucket == None:
			target_bucket = self.WorkingZone

		print(f"move data from {source_bucket} to {target_bucket}")

		#self.clean_bucket()

		for key in self.get_files(source_bucket):
			if key in config.get("FILES","NAME").split(",") and key not in self.get_files(target_bucket):
				copy_source = {"Bucket":source_bucket,"Key":key}
				self.S3.meta.client.copy(copy_source, target_bucket, key)



	def get_files(self, bucket_name):
		objects = []
		for my_bucket_object in self.S3.Bucket(bucket_name).objects.all():
			objects.append(my_bucket_object.key)
		print(objects)
		return objects
	
	def clean_bucket(self, bucket_name):
		logging.debug(f"Cleaning bucket : {bucket_name}")
		self.S3.Bucket(bucket_name).objects.all().delete()
