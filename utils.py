import pandas as pd

def create_excel(data, file_name, save_path):
      """
      Function for saving data to excel file
      """
      # Create a Pandas Excel writer using XlsxWriter as the engine.
      writer = pd.ExcelWriter(save_path + file_name, engine='xlsxwriter')
   
      # Convert the dataframe to an XlsxWriter Excel object.
      data.to_excel(writer, sheet_name='Sheet1', index=False)
   
      # Close the Pandas Excel writer and output the Excel file.
      writer.save()