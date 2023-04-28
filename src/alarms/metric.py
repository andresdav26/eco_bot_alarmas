import pandas as pd
from alarms.preprocess import DataFramepreprocess

class Metric:
    def __init__(self,df):
        self.df = df

    def metricEstado(self):
        tempEst = [self.df.groupby(by=["Radicado","Estado"])["Estado"].count().reset_index(0).rename(columns={'Estado':'Reprocesos estado'}), # procesos por estados 
                    self.df.groupby(by=['Radicado', 'Estado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Días estado'}).round(4)] # días por estado

        return tempEst
    
    def metricCombEstado(self):
        tempComb = [self.df.groupby(by=["Radicado","Combinacion estado"])["Combinacion estado"].count().reset_index(0).rename(columns={'Combinacion estado':'Procesos combinación estado'})] # procesos por combinación 

        return tempComb

    def metricRadicado(self):
        temp = self.df.groupby(by=["Radicado","Estado"])["Estado"].count().reset_index(0).rename(columns={'Estado':'Reprocesos estado'})
        tempRad = [temp.groupby(by=["Radicado"]).sum().reset_index(0).rename(columns={'Reprocesos estado':'Veces radicado'}), # procesos por radicado (sumatoria de los procesos que contiene cada estado)
                    temp.groupby(by=["Radicado"]).count().reset_index(0).rename(columns={'Reprocesos estado':'Estados radicado'}),  # cantidad estados por radicado
                    self.df.groupby(by=['Radicado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Días radicado'}).round(4)] # días por radicado

        return tempRad