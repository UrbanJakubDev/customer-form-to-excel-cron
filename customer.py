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

        data = requests.get(self.url + self.resource,
                            auth=BearerAuth(self.token)).json()
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
        # Load .env file
        load_dotenv()
        self.URL = os.getenv('URL')
        self.TOKEN = os.getenv('TOKEN')
        self.save_path = 'data/'
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

    def create_excel(self, resource):
        request = Request(self.URL, self.TOKEN, resource)
        df = request.get_data()

        # If self.file dir does not exist, create it
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)

        df.to_excel(self.save_path + resource + '.xlsx')

        # Log status to console with actual date and time of execution
        logging.info('Excel file for resource: ' + resource +
                     ' created at ' + time.strftime("%c"))
