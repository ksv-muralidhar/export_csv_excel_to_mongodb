import pymongo
import pandas as pd


class CsvXlToMongo:
    def __init__(self, file: str, host: str, database: str, collection: str):
        self.file = file
        self.host = host
        self.database = database
        self.collection = collection
        self.__client = None
        self.__input_df = None
        self.__parsed_data = None
        self.__error = 0

    def __read_input_data(self):
        try:
            if (self.file.endswith('.xlsx')) | (self.file.endswith('.xls')):
                self.__input_df = pd.read_excel(self.file, parse_dates=True)
            elif self.file.endswith('.csv'):
                self.__input_df = pd.read_csv(self.file, sep=',', parse_dates=True)
            else:
                raise Exception('Invalid file format')
        except Exception as file_error:
            self.__error = 1
            print(f'UNABLE TO READ INPUT FILE\nReason: {file_error}')
        else:
            print('INPUT FILE READ SUCCESSFUL')

    def __connect(self):
        try:
            self.__client = pymongo.MongoClient(self.host)
            _ = self.__client.list_database_names()
        except Exception as conn_exception:
            self.__error = 1
            self.__client = None
            print(f'UNABLE TO CONNECT TO DB SERVER\nREASON: {conn_exception}')
        else:
            print('DB SERVER CONNECTION SUCCESSFUL')

    def __parse_input_data(self):
        self.__parsed_data = []
        try:
            for row in self.__input_df.itertuples():
                row_dict = row._asdict()
                _ = row_dict.pop('Index')
                self.__parsed_data.append(row_dict)
        except Exception as parse_err:
            self.__error = 1
            print(f'UNABLE TO PARSE INPUT FILE\nReason: {parse_err}')
        else:
            print('INPUT DATA PARSE SUCCESSFUL')

    def __bulk_insert(self):
        try:
            db = self.__client[self.database]
            coll = db[self.collection]
            coll.insert_many(self.__parsed_data)
        except Exception as insert_err:
            self.__error = 1
            print(f'DB INSERT OPERATION FAILED\nReason: {insert_err}')
        else:
            print('DB INSERT OPERATION SUCCESSFUL')

    def __close_connection(self):
        if self.__client is not None:
            self.__client.close()
            self.__client = None
            print('DB CONNECTION CLOSED')

    def load_to_db(self):
        if self.__error == 0:
            self.__connect()
        if self.__error == 0:
            self.__read_input_data()
        if self.__error == 0:
            self.__parse_input_data()
        if self.__error == 0:
            self.__bulk_insert()
        if self.__client is not None:
            self.__close_connection()
        if self.__error == 0:
            print('TASK COMPLETE')
        if self.__error == 1:
            print('TASK INCOMPLETE')
