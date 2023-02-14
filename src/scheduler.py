import logging
from datetime import datetime
from time import sleep

from apscheduler.schedulers.blocking import BlockingScheduler

from alarm_bot import auto_alarms

sche_logger = logging.getLogger('apscheduler')
sche_logger.setLevel(logging.CRITICAL)
sche_logger.propagate = False

scheduler = BlockingScheduler()
scheduler.add_job(auto_alarms, 'interval', seconds=10, max_instances=1)

scheduler.start()

for job in scheduler.get_jobs():
    job.modify(next_run_time=datetime.now())

while True:
    sleep(1)
