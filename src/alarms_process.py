import datetime
import random

import numpy as np
import pandas as pd

from utils import calculate_times


def find_outliers_IQR(val):
    q75, q25  = np.percentile(val.values, [75, 25])
    IQR = q75 - q25
    th = q75 + 1.5*IQR
    outliers = (val > th).sum()
    if (val > th).sum() > 0:
        MAX = np.max(val.values[val.values>th])
    else:
        MAX = None
    return outliers, th, MAX


def find_alerts(dataMongo):
    # Definir dataframe 
    df = pd.DataFrame(dataMongo)
    df['Radicado'] = df['Radicado'].astype(str)
    df['Combinacion estado'] = df['Estado']+'-'+df['Estado Destino'] 

    df['Fecha Inicio / Hora'] = df['Fecha Inicio / Hora'] - datetime.timedelta(hours=5)
    df['Fecha Fin / Hora'] = df['Fecha Fin / Hora'] - datetime.timedelta(hours=5)
    
    # Días laborados por registro
    df = calculate_times(df)

    # variables 
    total_rad = df['Radicado'].unique() # Total Radicados 
    estados = set(df['Estado'].unique().tolist() + df['Estado Destino'].unique().tolist()) # Estados
    comb_estado = df['Combinacion estado'].unique() # Combinación de estados

    temp_Est = [df.groupby(by=["Radicado","Estado"])["Estado"].count().reset_index(0).rename(columns={'Estado':'Procesos estado'}), # procesos por estados 
            df.groupby(by=['Radicado', 'Estado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Dias estado'})] # dias por estado

    temp_Comb = df.groupby(by=["Radicado","Combinacion estado"])["Combinacion estado"].count().reset_index(0).rename(columns={'Combinacion estado':'Procesos combinacion estado'}) # procesos por combinación 

    temp_Rad = [temp_Est[0].groupby(by=["Radicado"]).sum().reset_index(0).rename(columns={'Procesos estado':'Procesos radicado'}), # procesos por radicado
                temp_Est[0].groupby(by=["Radicado"]).count().reset_index(0).rename(columns={'Procesos estado':'Cantidad estados x radicado'}),  # cantidad estados por radicado
                df.groupby(by=['Radicado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Dias radicado'})] # dias por radicado 

    # MAIN 
    results = []
    
    # ANÁLISIS POR ESTADO 
    variables = ['Procesos estado', 'Dias estado']
    type_alert = ['Reprocesos por estado','Dias laborados por estado']
    rads_in = df.groupby(by=["Estado Destino"])["Radicado"].count()
    rads_out = df.groupby(by=["Estado"])["Radicado"].count()
    for st in estados:
        alerts = []
        result = {}
        dif = 0
        result['Tipo_de_analisis'] = 'Estado'
        result['Nombre'] = st
        for i, v in enumerate(variables):
            if st in temp_Est[i].index:
                val = temp_Est[i].loc[[st]][v] # procesos del estado "st" o días del estado "st"
                outliers, th, MAX = find_outliers_IQR(val) # cantidad de radicados atípicos, threshold, valor máximo 
                
                if outliers > 0:
                    # diferencia in/out radicados
                    if (st in rads_in.index) and (st in rads_out.index):
                        Rin = rads_in.loc[[st]].item()
                        Rout = rads_out.loc[[st]].item()
                        dif = Rin - Rout
                        
                    # porcentaje
                    radicados = temp_Est[i].loc[[st]]['Radicado'].unique() # total radicados donde aparece el estado
                    porcentaje = outliers/len(radicados)*100

                    # porcentaje de radicados del proceso sobre los radicados totales 
                    result['rads/total_rad'] = round(len(radicados)/len(total_rad)*100, 2)

                    # radicados donde hay casos atípicos 
                    ejemplo_rad_max = temp_Est[i].loc[[st]]['Radicado'][val==MAX].unique() # radicado donde se encuentra el máximo valor
                    
                    inf_alert = {'Alerta': type_alert[i],
                                'Total_radicados':int(len(radicados)),
                                'Umbral':float(th),
                                'Cantidad_radicados_atipicos':int(outliers),
                                'Proporcion':round(float(porcentaje),2),
                                'Ejemplo_radicado_atipico':random.choice(ejemplo_rad_max),
                                'Maximo':float(MAX),
                                }
                    
                    alerts.append(inf_alert)
        # Diferencia de radicados in/out
        if dif > 0: 
            inf_alert = {'Alerta': 'Diferencia de radicados in/out',
                        'Diferencia': dif}
            alerts.append(inf_alert)

        result['Alertas'] = alerts
        if len(alerts) > 0: 
            results.append(result)

    
    # ANÁLISIS POR COMBINACIÓN DE ESTADOS
    variables = ['Procesos combinacion estado']
    type_alert = ['Reprocesos por combinacion de estados']
    for cst in comb_estado:
        alerts = []
        result = {}
        result['Tipo_de_analisis'] = 'Combinacion de estados'
        result['Nombre'] = cst
        for i, v in enumerate(variables):
            if cst in temp_Comb.index:
                val = temp_Comb.loc[[cst]][v] # procesos de comb_estado "cst"
                outliers, th, MAX = find_outliers_IQR(val) 
                
                if outliers > 0:
                    # porcentaje
                    radicados = temp_Comb.loc[[cst]]['Radicado'].unique() # radicados donde aparece comb_st
                    porcentaje = outliers/len(radicados)*100

                    # porcentaje de radicados del proceso sobre los radicados totales 
                    result['rads/total_rad'] = round(len(radicados)/len(total_rad)*100, 2)

                    # radicados donde hay casos atípicos 
                    ejemplo_rad_max = temp_Comb.loc[[cst]]['Radicado'][val==MAX].unique() # radicado donde se encuentra el máximo valor
                        
                    inf_alert = {'Alerta': type_alert[i],
                                'Total_radicados':int(len(radicados)),
                                'Umbral':float(th),
                                'Cantidad_radicados_atipicos':int(outliers),
                                'Proporcion':round(float(porcentaje),2),
                                'Ejemplo_radicado_atipico':random.choice(ejemplo_rad_max),
                                'Maximo':float(MAX),
                                }
                    
                    alerts.append(inf_alert)

        result['Alertas'] = alerts
        if len(alerts) > 0:
            results.append(result)       

    # ANÁLISIS POR RADICADO
    alerts = []
    result = {}
    result['Tipo_de_analisis'] = 'Radicado'
    result['Nombre'] = 'No aplica'
    variables = ['Procesos radicado','Cantidad estados x radicado','Dias radicado']
    type_alert = ['Veces radicado','Estados radicado','Dias laborados por radicado']
    for i, v in enumerate(variables):
        val = temp_Rad[i][v] # dias de los radicados 
        outliers, th, MAX = find_outliers_IQR(val) 

        if outliers > 0:
            
            # porcentaje
            porcentaje = outliers/len(total_rad)*100

            # porcentaje de radicados sobre los radicados totales 
            result['rads/total_rad'] = round(len(total_rad)/len(total_rad)*100, 2)
            
            # radicados donde hay casos atípicos 
            ejemplo_rad_max = temp_Rad[i]['Radicado'][val==MAX].unique() # radicado donde se encuentra el máximo valor
            
            inf_alert = {'Alerta': type_alert[i],
                        'Total_radicados':int(len(total_rad)),
                        'Umbral':float(th),
                        'Cantidad_radicados_atipicos':int(outliers),
                        'Proporcion':round(float(porcentaje),2),
                        'Ejemplo_radicado_atipico':random.choice(ejemplo_rad_max),
                        'Maximo':float(MAX),
                        }
                    
            alerts.append(inf_alert)
  
    result['Alertas'] = alerts
    if len(alerts) > 0:
        results.append(result) 

    return sorted(results, key = lambda x: x['rads/total_rad'], reverse=True)
