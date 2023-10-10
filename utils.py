import os
import pandas as pd

def create_excel(data: pd.DataFrame, file_name:str, save_path:str):
      """
      Function for saving data to excel file
      """
      file_path = os.path.join(save_path, file_name)
      
      os.makedirs(save_path, exist_ok=True)
      
      data.to_excel(file_path, index=False)
   