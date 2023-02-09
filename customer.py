import time
import os
import pandas as pd
import requests
from dotenv import load_dotenv

import logging

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


"""
Request class
param: URL, TOKEN, resource
"""
class Request():

    def __init__(self, url, token, resource):
        self.url = url
        self.token = token
        self.resource = resource

    # Get the data from the API and return it as a dataframe
    def get_data(self):
        
        data = requests.get(self.url + self.resource,auth=BearerAuth(self.token)).json()
        df = pd.DataFrame(data)

        # Convert column ico to numeric
        if self.resource == "products":
            df['trader'] = df['trader'].apply(pd.to_numeric)

        df['ico'] = df['ico'].apply(pd.to_numeric)
        return df


"""
Function for saving data to excel file
"""
class CustomerForm():
   
   def __init__(self):
      self.resources = ['products', 'purchases']
      self.interval = '*/1'

   def create_jobs(self, aps):
      for resource in self.resources:
         aps.add_job(
         self.create_excel,
         trigger='cron',
         minute=self.interval,
         id=resource,
         args=[resource],
         replace_existing=True)

         # Logging
         logging.info('Job for resource: ' + resource + ' created')



   @staticmethod
   def create_excel(resource):

      # Load .env file
      load_dotenv()
      URL = os.getenv('URL')
      TOKEN = os.getenv('TOKEN')
      save_path = 'data/'

      # Create request
      request = Request(URL, TOKEN, resource + '/excel')
      df = request.get_data()
      df.to_excel(save_path + resource + '.xlsx')

      # Log status to console with actual date and time of execution
      logging.info('Excel file for resource: ' + resource + ' created at ' + time.strftime("%c"))
      