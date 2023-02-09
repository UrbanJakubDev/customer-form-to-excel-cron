import os
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from customer import CustomerForm

import logging
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



    # Start scheduler
    scheduler.start()
    print('Scheduler started')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # Run forever
    while True:
        time.sleep(2)


if __name__ == '__main__':
    main()
