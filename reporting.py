# Import database connection
import datetime
from database import DBConnection

# Import pandas to create the dataframe
import pandas as pd
from typing import List, Union
import logging

# Import the connection engine


class Reporting():
   
      def __init__(self, output_file_path: str):
         self.engine = DBConnection.getInstance().getEngine() 
         self.output_file_path = output_file_path
   
      def get_report_data(self, querry: str) -> pd.DataFrame:
         df = pd.read_sql(querry, self.engine)

         time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
         logging.info(f'Created dataframe at {time}')
         return df
   
      def create_xlsx_report(self) -> None:
         df = self.get_report_data()
         df.to_excel(self.output_file_path, index=False)

      





   

   