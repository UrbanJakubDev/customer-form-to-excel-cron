import os
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from src.customer import CustomerForm
from src.gas_watcher import GasWatcher

import logging

# Logging to file
logging.basicConfig(filename='logs.log', level=logging.DEBUG)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)






def main():

    # APScheduler settings
    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
    }

    executors = {
        'default': ThreadPoolExecutor(2),
        'processpool': ProcessPoolExecutor(2)
    }

    scheduler = BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        timezone="Europe/Prague")
    


    # Add jobs to scheduler
    customer = CustomerForm()
    customer.create_jobs(scheduler)
    
    gw = GasWatcher()
    gw.create_job(scheduler)



    # Start scheduler
    scheduler.start()
    print('Scheduler started')
    print('Running jobs: ', scheduler.get_jobs())
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # Run forever
    while True:
        pass


if __name__ == '__main__':
    main()
