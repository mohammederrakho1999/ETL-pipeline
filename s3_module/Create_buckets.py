import boto3
import uuid


s3_resource = boto3.resource('s3')

def create_bucket_name(bucket_prefix):
    # The generated bucket name must be between 3 and 63 chars long
    return ''.join([bucket_prefix, str(uuid.uuid4())])

bucket_name = create_bucket_name("landingzone1999")
bucket_name2 = create_bucket_name("workingzone1999")
bucket_name3 = create_bucket_name("processedzone1999")


s3_resource.create_bucket(Bucket = bucket_name,
                          CreateBucketConfiguration = {
                              'LocationConstraint': 'eu-west-1'})
s3_resource.create_bucket(Bucket = bucket_name2,
                          CreateBucketConfiguration = {
                              'LocationConstraint': 'eu-west-1'})
s3_resource.create_bucket(Bucket = bucket_name3,
                          CreateBucketConfiguration = {
                              'LocationConstraint': 'eu-west-1'})








