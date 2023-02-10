from time import sleep
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from alarms import auto_alarms

scheduler = BackgroundScheduler()

scheduler.add_job(auto_alarms, 'interval', seconds=30, max_instances=1)

scheduler.start()

for job in scheduler.get_jobs():
    job.modify(next_run_time=datetime.now())

while True:
    sleep(1)
