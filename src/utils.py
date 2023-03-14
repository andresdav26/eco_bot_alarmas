from datetime import datetime, timedelta
import numpy as np
from pathlib import Path
import pandas as pd

HOLIDAYS = []
with open(Path(__file__).parent / 'holidays.txt', 'r') as f:
    HOLIDAYS = [datetime.strptime(l.strip(), '%Y-%m-%d').date() for l in f.readlines()]


def datetimes_hours_difference(df_end: pd.Series, df_start: pd.Series) -> pd.Series:
    """
    Calculate the total hours difference between two Pandas Series
    containing datetime values (df_end - df_start)

    Args:
        df_end (pd.Series): Contains datetime values
        df_start (pd.Series): Contains datetime values

    Returns:
        df_date_diff (pd.Series): Difference between df_end and df_start
    """
    df_start_hours = df_start.dt.ceil('d')
    df_end_hours = df_end.dt.floor('d')
    one_day_mask = df_start.dt.floor('d') == df_end_hours

    df_days_hours = np.busday_count(
        df_start_hours.values.astype('datetime64[D]'),
        df_end_hours.values.astype('datetime64[D]'),
        weekmask=[1,1,1,1,1,1,0],
        holidays=HOLIDAYS
        )
    df_days_hours = df_days_hours * 24

    mask1 = df_start.dt.dayofweek != 6
    hours1 = df_start_hours - df_start.dt.floor('min')
    hours1.loc[~mask1] = pd.NaT

    df_start_hours = hours1 / pd.to_timedelta(1, unit='H')
    df_start_hours = df_start_hours.fillna(0)

    mask2 = df_end.dt.dayofweek != 6
    hours2 = df_end.dt.floor('min') - df_end_hours
    hours2.loc[~mask2] = pd.NaT

    df_end_hours = hours2 / pd.to_timedelta(1, unit='H')
    df_end_hours = df_end_hours.fillna(0)

    df_date_diff = df_start_hours + df_end_hours + df_days_hours
    one_day = (df_end.dt.floor('min') - df_start.dt.floor('min'))
    one_day = one_day / pd.to_timedelta(1, unit='H')
    df_date_diff = df_date_diff.mask(one_day_mask, one_day)

    return df_date_diff

def calculate_times(df: pd.DataFrame):
    # df = df.reset_index(drop=False) # becasue case ids are index an has duplicate values
    df['tiempo_estado'] = datetimes_hours_difference(df['Fecha Fin / Hora'], df['Fecha Inicio / Hora'])
    df['tiempo_estado'] = df['tiempo_estado'] / 24 # hours to days

    return df['tiempo_estado']


def time_string(days: float):
    hours = (days % 1) * 24
    minutes = (hours % 1) * 60
    seconds = (minutes % 1) * 60

    return f'{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s'