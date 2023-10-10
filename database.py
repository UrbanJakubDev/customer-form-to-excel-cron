
# SQL Alchemy conection engine to the database MYSQL
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Singleton class to connect to the database


class DBConnection():

    __instance = None

    @staticmethod
    def getInstance():
        if DBConnection.__instance == None:
            DBConnection()
        return DBConnection.__instance

    def __init__(self):
        if DBConnection.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            DBConnection.__instance = self
            load_dotenv()
            self.__engine = create_engine(self.getConnectionString())

    # Get the connection engine
    def getEngine(self):
        return self.__engine

    # Load .env and create the connection string
    def getConnectionString(self):
        load_dotenv()
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        return f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}'
