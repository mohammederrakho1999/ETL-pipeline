import configparser

config = configparser.ConfigParser()
config.read_file(open("warehouse_config.cfg"))


staging_area_schema = config.get('STAGING', 'SCHEMA')
s3_processed_zone = 's3://' + config.get('BUCKET', 'PROCESSED_ZONE1999')
iam_role = config.get('IAM_ROLE', 'ARN')


create_staging_schema = "CREATE SCHEMA IF NOT EXISTS {};".format(staging_area_schema)


drop_books_table = "DROP TABLE IF EXISTS {}.books;".format(staging_area_schema)
drop_ratings_table = "DROP TABLE IF EXISTS {}.ratings;".format(staging_area_schema)
drop_users_table = "DROP TABLE IF EXISTS {}.users;".format(staging_area_schema)



create_books_table = """
CREATE TABLE IF NOT EXISTS {}.books
(
    "ISBN" BIGINT PRIMARY KEY,
    "Book-Title" VARCHAR,
    "Book-Author" VARCHAR,
    "Year-Of-Publication" INT,
    "Publisher" VARCHAR,
    "Image-URL-S" VARCHAR,
    "Image-URL-M" VARCHAR,
    "Image-URL-L" VARCHAR
)
DISTSTYLE ALL
;
""".format(staging_area_schema)


create_ratings_table = """
CREATE TABLE IF NOT EXISTS {}.ratings
(
    "User-ID" INT PRIMARY KEY ,
    "ISBN" BIGINT,
    "Book-Rating" INT
)
DISTSTYLE ALL
;
""".format(staging_area_schema)

create_users_table = """
CREATE TABLE IF NOT EXISTS {}.users
(
    "User-ID" INT PRIMARY KEY ,
    "Age" INT,
    "City" VARCHAR,
    "State" VARCHAR,
    "country" VARCHAR 
)
DISTSTYLE ALL
;
""".format(staging_area_schema)



copy_books_table = """
COPY {0}.books
FROM '{1}/books'
IAM_ROLE '{2}'
CSV
GZIP
NULL AS  '\\000'
IGNOREHEADER 1
;
""".format(staging_area_schema, s3_processed_zone, iam_role)


copy_ratings_table = """
COPY {0}.ratings
FROM '{1}/book_ratings'
IAM_ROLE '{2}'
CSV
GZIP
NULL AS  '\\000'
IGNOREHEADER 1
;
""".format(staging_area_schema, s3_processed_zone, iam_role)


copy_users_table = """
COPY {0}.users
FROM '{1}/Usersbooks'
IAM_ROLE '{2}'
CSV
GZIP
NULL AS  '\\000'
IGNOREHEADER 1
;
""".format(staging_area_schema, s3_processed_zone, iam_role)

