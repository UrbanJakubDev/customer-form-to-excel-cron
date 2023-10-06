import os
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from customer import CustomerForm

import logging

from gas_watcher import GasWatcher
# Logging to file
logging.basicConfig(filename='logs.log', level=logging.DEBUG)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)






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

    # Create jobs
    customer = CustomerForm()
    customer.create_jobs(scheduler)
    
    # Add jobs to scheduler
    gw = GasWatcher()
    scheduler.add_job(gw.run, 'interval', minutes=1)



    # Start scheduler
    scheduler.start()
    print('Scheduler started')
    print('Running jobs: ', scheduler.get_jobs())
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # Run forever
    while True:
        time.sleep(2)


if __name__ == '__main__':
    main()
