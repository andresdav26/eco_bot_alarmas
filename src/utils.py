from datetime import datetime, timedelta
import numpy as np
from pathlib import Path

HOLIDAYS = []
with open(Path(__file__).parent / 'holidays.txt', 'r') as f:
    HOLIDAYS = [datetime.strptime(l.strip(), '%Y-%m-%d').date() for l in f.readlines()]


def calculate_date_diff(init_date: datetime, end_date: datetime):

    if (end_date - init_date).days > 0 and (init_date.weekday() == 6 or init_date.date() in HOLIDAYS):
        init_date = init_date + timedelta(days=1)
        init_date = init_date.replace(hour=0, minute=0, second=0, microsecond=0)

    total_diff = end_date - init_date
    total_hours_diff = (total_diff.days * 24) + (total_diff.seconds / 3600)
    total_days_diff = end_date.date() - init_date.date()
    # count busy days, including saturday
    busy_days = np.busday_count(init_date.date(), end_date.date(), weekmask=[1, 1, 1, 1, 1, 1, 0], holidays=HOLIDAYS)
    off_days = total_days_diff.days - busy_days

    return round((total_hours_diff - (off_days * 24)) / 24, 8)


def time_string(days: float):
    hours = (days % 1) * 24
    minutes = (hours % 1) * 60
    seconds = (minutes % 1) * 60

    return f'{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s'