import time
import os
import pandas as pd
import requests
from dotenv import load_dotenv

import logging

from utils import create_excel


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
        """
        Initializes a Request object with the given url, token, and resource.

        Args:
        url (str): The base URL of the API.
        token (str): The authentication token for the API.
        resource (str): The name of the resource to retrieve data from.
        """
        self.url = url
        self.token = token
        self.resource = resource

    def get_data(self):
        """
        Sends a GET request to the API and returns the response data as a pandas DataFrame.

        Returns:
        pandas.DataFrame: The response data as a DataFrame.
        """
        res = requests.get(f"{self.url}{self.resource}/excel", auth=BearerAuth(self.token))
        data = res.json()
        df = pd.DataFrame(data)

        # Convert column ico to numeric
        if self.resource == "products":
            df['trader'] = pd.to_numeric(df['trader'])

        df['ico'] = pd.to_numeric(df['ico'])
        return df


class CustomerForm():
    """
    A class used to represent a customer form.

    ...

    Attributes
    ----------
    URL : str
        the URL of the API
    TOKEN : str
        the token to access the API
    save_path : str
        the path to save the excel files
    resources : list
        a list of resources to create excel files for
    interval : str
        the interval at which to create the excel files

    Methods
    -------
    create_jobs(aps)
        Creates jobs for each resource in the resources list
    create_excel(resource)
        Creates an excel file for the given resource
    """

    def __init__(self):
        """
        Constructs all the necessary attributes for the customer form object.
        """
        # Load .env file
        load_dotenv()
        self.URL = os.getenv('URL')
        self.TOKEN = os.getenv('TOKEN')
        self.save_path = os.getenv('FILE_STORAGE_PATH')
        self.resources = ['products', 'purchases']
        self.interval = '*/15'

    def create_jobs(self, aps):
        """
        Creates jobs for each resource in the resources list.

        Parameters
        ----------
        aps : BackgroundScheduler
            the scheduler to use for creating the jobs
        """
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
        """
        Creates an excel file for the given resource.

        Parameters
        ----------
        resource : str
            the resource to create the excel file for
        """
        # Make request to API
        request = Request(self.URL, self.TOKEN, resource)
        df = request.get_data()

        create_excel(df, f'{resource}.xlsx', self.save_path)

        # Log status to console with actual date and time of execution
        message = f"Excel file for resource: {resource} created at {time.strftime('%c')}"
        logging.info(message)
