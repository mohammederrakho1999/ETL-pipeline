import configparser

config = configparser.ConfigParser()
config.read_file(open("warehouse_config.cfg"))


warehouse_schema  = config.get('WAREHOUSE', 'SCHEMA')
create_warehouse_schema = "CREATE SCHEMA IF NOT EXISTS {};".format(warehouse_schema)


drop_books_table = "DROP TABLE IF EXISTS {}.books;".format(warehouse_schema)
drop_ratings_table = "DROP TABLE IF EXISTS {}.ratings;".format(warehouse_schema)
drop_users_table = "DROP TABLE IF EXISTS {}.users;".format(warehouse_schema)



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
""".format(warehouse_schema)


create_ratings_table = """
CREATE TABLE IF NOT EXISTS {}.ratings
(
    "User-ID" INT PRIMARY KEY ,
    "ISBN" BIGINT,
    "Book-Rating" INT
)
DISTSTYLE ALL
;
""".format(warehouse_schema)

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
""".format(warehouse_schema)