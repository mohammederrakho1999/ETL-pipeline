import configparser

config = configparser.ConfigParser()
config.read_file(open("warehouse_config.cfg"))

staging_area_schema = config.get('STAGING', 'SCHEMA')
warehouse_schema = config.get('WAREHOUSE', 'SCHEMA')




upsert_books = """
BEGIN TRANSACTION;
DELETE FROM {1}.books
using {0}.books
where {1}.books.ISBN = {0}.books.ISBN;
INSERT INTO {1}.books
SELECT * FROM {0}.books;
END TRANSACTION ;
COMMIT;
""".format(staging_area_schema, warehouse_schema)



upsert_ratings = """
BEGIN TRANSACTION;
DELETE FROM {1}.ratings
using {0}.ratings
where {1}.ratings.User-ID = {0}.ratings.User-ID;
INSERT INTO {1}.ratings
SELECT * FROM {0}.ratings;
END TRANSACTION ;
COMMIT;
""".format(staging_area_schema, warehouse_schema)


upsert_users = """
BEGIN TRANSACTION;
DELETE FROM {1}.users
using {0}.users
where {1}.users.User-ID = {0}.users.User-ID;
INSERT INTO {1}.users
SELECT * FROM {0}.users;
END TRANSACTION ;
COMMIT;
""".format(staging_area_schema, warehouse_schema)




upsert_queries = [upsert_books, upsert_ratings, upsert_users]