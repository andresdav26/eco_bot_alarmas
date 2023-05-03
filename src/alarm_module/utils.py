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

def find_outliers_IQR(val):
    q75, q25  = np.percentile(val, [75, 25])
    IQR = q75 - q25
    th = q75 + 1.5*IQR
    
    return th

def preprocess(logsDataframe):
        df = logsDataframe.reset_index(drop=True)
        df['Radicado'] = df['Radicado'].astype(str)
        df['Combinacion estado'] = df['Estado']+'-'+df['Estado Destino'] 

        df['Fecha Inicio / Hora'] = df['Fecha Inicio / Hora'] - timedelta(hours=5)
        df['Fecha Fin / Hora'] = df['Fecha Fin / Hora'] - timedelta(hours=5)
        
        # Días laborados por registro
        df['tiempo_estado'] = calculate_times(df)

        tempEst = [df.groupby(by=["Radicado","Estado"])["Estado"].count().reset_index(0).rename(columns={'Estado':'Reprocesos estado'}), # procesos por estados 
                   df.groupby(by=['Radicado', 'Estado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Días estado'}).round(4)] # días por estado

        tempComb = [df.groupby(by=["Radicado","Combinacion estado"])["Combinacion estado"].count().reset_index(0).rename(columns={'Combinacion estado':'Procesos combinación estado'})] # procesos por combinación 

        tempRad = [tempEst[0].groupby(by=["Radicado"]).sum().reset_index(0).rename(columns={'Reprocesos estado':'Veces radicado'}), # procesos por radicado (sumatoria de los procesos que contiene cada estado)
                    tempEst[0].groupby(by=["Radicado"]).count().reset_index(0).rename(columns={'Reprocesos estado':'Estados radicado'}),  # cantidad estados por radicado
                    df.groupby(by=['Radicado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Días radicado'}).round(4)] # días por radicado
        # servicios
        df_serv1 = df[['Estado', 'Servicio']].drop_duplicates().rename(columns={'Estado': 'estado'})
        df_serv2 = df[['Estado Destino', 'Servicio']].drop_duplicates().rename(columns={'Estado Destino': 'estado'})
        df_serv = pd.concat([df_serv1, df_serv2], ignore_index=True).drop_duplicates(subset='estado')
        service_dict = {row['estado']: row['Servicio'] for _, row in df_serv.iterrows()}

        return df, service_dict, tempEst, tempComb, tempRad


