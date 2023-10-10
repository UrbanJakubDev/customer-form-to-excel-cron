import os
from database import DBConnection
import pandas as pd
from dotenv import load_dotenv

from utils import create_excel


class GasWatcher():
   
   def __init__(self) -> None:
      self.db = DBConnection().getInstance()
      self.save_path = os.getenv('FILE_STORAGE_PATH')
   
   def run(self):
      data = self.get_data()
      self.save_data(data)
      
      
   def get_data(self):
      engine = self.db.getEngine()
      
      sql = "SELECT * FROM kgj_const"
      df = pd.read_sql(sql, engine)
      return df
   
   def save_data(self, df):
      
      create_excel(df, 'gas.xlsx', self.save_path)