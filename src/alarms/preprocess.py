import pandas as pd
from datetime import timedelta
from alarms.utils import calculate_times


class DataFramepreprocess:

    def __init__(self,logsDataframe):
        self.logsDataframe = logsDataframe

    def preprocess(self):
        df = self.logsDataframe.reset_index(drop=True)
        df['Radicado'] = df['Radicado'].astype(str)
        df['Combinacion estado'] = df['Estado']+'-'+df['Estado Destino'] 

        df['Fecha Inicio / Hora'] = df['Fecha Inicio / Hora'] - timedelta(hours=5)
        df['Fecha Fin / Hora'] = df['Fecha Fin / Hora'] - timedelta(hours=5)
        
        # Días laborados por registro
        df['tiempo_estado'] = calculate_times(df)

        # servicios
        df_serv1 = df[['Estado', 'Servicio']].drop_duplicates().rename(columns={'Estado': 'estado'})
        df_serv2 = df[['Estado Destino', 'Servicio']].drop_duplicates().rename(columns={'Estado Destino': 'estado'})
        df_serv = pd.concat([df_serv1, df_serv2], ignore_index=True).drop_duplicates(subset='estado')
        service_dict = {row['estado']: row['Servicio'] for _, row in df_serv.iterrows()}

        return df, service_dict