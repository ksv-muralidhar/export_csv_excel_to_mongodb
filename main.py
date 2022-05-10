from xlcsv_mongo import CsvXlToMongo

mongo = CsvXlToMongo(file="file_location",
                     host='localhost:27017',
                     database="db_name",
                     collection="collection_name")

mongo.load_to_db()
