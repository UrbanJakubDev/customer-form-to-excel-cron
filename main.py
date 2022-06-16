import os
import pandas as pd
import requests
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import os
from dotenv import load_dotenv


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
        response = requests.get(self.url + self.resource,
                                auth=BearerAuth(self.token))
        data = response.json()
        df = pd.DataFrame(data)

        # Convert column ico to numeric
        if self.resource == "products":
            df['trader'] = df['trader'].apply(pd.to_numeric)

        df['ico'] = df['ico'].apply(pd.to_numeric)
        return df


"""
Function for saving data to excel file
"""


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

    # Print status to console with actual date and time of execution
    print('Job for resource: ' + resource + ' executed at: ' +
          time.strftime("%Y-%m-%d %H:%M:%S"))
    return df


def main():

    # Job store SQLite for scheduler
    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
    }

    # Executors
    executors = {
        'default': ThreadPoolExecutor(2),
        'processpool': ProcessPoolExecutor(2)
    }

    # Scheduler
    scheduler = BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        timezone="Europe/Prague")

    resources = ['products', 'purchases']

    # Create jobs
    for resource in resources:
        interval = '*/1'
        print('Creating job for resource: ' + resource +
              ' with interval: Minute=' + str(interval))

        scheduler.add_job(
            create_excel,
            trigger='cron',
            minute=interval,
            id=resource,
            args=[resource])

    # Start scheduler
    scheduler.start()
    print('Scheduler started')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # Run forever
    while True:
        time.sleep(2)


if __name__ == '__main__':
    main()
