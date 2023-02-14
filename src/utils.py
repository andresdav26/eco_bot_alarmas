from datetime import datetime, timedelta
import numpy as np
from pathlib import Path
import pandas as pd

HOLIDAYS = []
with open(Path(__file__).parent / 'holidays.txt', 'r') as f:
    HOLIDAYS = [datetime.strptime(l.strip(), '%Y-%m-%d').date() for l in f.readlines()]


def calculate_times(df: pd.DataFrame):
    df['delta_days'] = (df['Fecha Fin / Hora'] - df['Fecha Inicio / Hora']).dt.days
    df['new_init_date'] = df['Fecha Inicio / Hora']
    df['is_holiday'] = df['Fecha Inicio / Hora'].dt.date.isin(HOLIDAYS)
    mask = (df['delta_days'] > 0) & ((df['Fecha Inicio / Hora'].dt.weekday == 6) | (df['is_holiday'] == True))
    df.loc[mask,'new_init_date'] = df['new_init_date'] + timedelta(days=1)
    df.loc[mask,'new_init_date'] = df['new_init_date'].dt.normalize() # set date to 00h00m00s000ms
    df['offdays'] = np.busday_count(
        df['new_init_date'].values.astype('datetime64[D]'), df['Fecha Fin / Hora'].values.astype('datetime64[D]'),
        weekmask=[1,1,1,1,1,1,0],
        holidays=HOLIDAYS
        )
    df['offdays'] = df['delta_days'] - df['offdays']
    df['tiempo_estado'] = (df['Fecha Fin / Hora'] - df['new_init_date']).dt.total_seconds() / (3600*24)
    mask = df['offdays'] > 0
    df.loc[mask, 'tiempo_estado'] = df['tiempo_estado'] - df['offdays']

    df = df.drop(['delta_days', 'new_init_date', 'is_holiday', 'offdays'], axis=1)

    return df


def time_string(days: float):
    hours = (days % 1) * 24
    minutes = (hours % 1) * 60
    seconds = (minutes % 1) * 60

    return f'{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s'