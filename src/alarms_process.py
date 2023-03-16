from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import collections

from utils import calculate_times


def find_outliers_IQR(val):
    q75, q25  = np.percentile(val, [75, 25])
    IQR = q75 - q25
    th = q75 + 1.5*IQR
    outliers = (val > th).sum()
    
    return outliers, th


def find_alerts(dataMongo, hist_data, config):
    # Definir dataframe 
    df = pd.DataFrame(dataMongo)
    df['Radicado'] = df['Radicado'].astype(str)
    df['Combinacion estado'] = df['Estado']+'-'+df['Estado Destino'] 

    df['Fecha Inicio / Hora'] = df['Fecha Inicio / Hora'] - timedelta(hours=5)
    df['Fecha Fin / Hora'] = df['Fecha Fin / Hora'] - timedelta(hours=5)
    
    # Días laborados por registro
    df['tiempo_estado'] = calculate_times(df)

    # variables 
    total_rad_periodo = len(df['Radicado'].unique()) # Total Radicados en el periodo
    estados = set(df['Estado'].unique().tolist() + df['Estado Destino'].unique().tolist()) # Estados
    comb_estado = df['Combinacion estado'].unique() # Combinación de estados

    temp_Est = [df.groupby(by=["Radicado","Estado"])["Estado"].count().reset_index(0).rename(columns={'Estado':'Procesos estado'}), # procesos por estados 
                df.groupby(by=['Radicado', 'Estado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Dias estado'})] # días por estado

    temp_Comb = df.groupby(by=["Radicado","Combinacion estado"])["Combinacion estado"].count().reset_index(0).rename(columns={'Combinacion estado':'Procesos combinacion estado'}) # procesos por combinación 

    temp_Rad = [temp_Est[0].groupby(by=["Radicado"]).sum().reset_index(0).rename(columns={'Procesos estado':'Veces radicado'}), # procesos por radicado (sumatoria de los procesos que contiene cada estado)
                temp_Est[0].groupby(by=["Radicado"]).count().reset_index(0).rename(columns={'Procesos estado':'Estados radicado'}),  # cantidad estados por radicado
                df.groupby(by=['Radicado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Dias radicado'})] # días por radicado

    # Serivicios 
    df_serv1 = df[['Estado', 'Servicio']].drop_duplicates().rename(columns={'Estado': 'estado'})
    df_serv2 = df[['Estado Destino', 'Servicio']].drop_duplicates().rename(columns={'Estado Destino': 'estado'})
    df_serv = pd.concat([df_serv1, df_serv2], ignore_index=True).drop_duplicates(subset='estado')
    service_dict = {row['estado']: row['Servicio'] for _, row in df_serv.iterrows()}

    # MAIN 
    results = []
    historial = []
    Nmuestras = int(5E5)

    # filtros de consulta
    cole = config['ColeccionLogs']
    proj = config['Proyecto']
    proc = config['Proceso']

    # ANÁLISIS POR ESTADO 
    variables = ['Procesos estado', 'Dias estado']
    # type_alert = ['Reprocesos por estado','Dias laborados por estado']
    rads_in = df.groupby(by=["Estado Destino"])["Radicado"].count()
    rads_out = df.groupby(by=["Estado"])["Radicado"].count()
    for st in estados:
        var = []
        hist = {}
        if st in df["Estado"].values:
            hist['Nombre'] = st
            
            # Diferencia radicados in/out en st
            if (st in rads_in.index) and (st in rads_out.index):
                Rin = rads_in.loc[[st]].item()
                Rout = rads_out.loc[[st]].item()
                dif = Rin - Rout
            else:
                dif = 0
            
            for i, v in enumerate(variables):
                result = {}

                # Radicados donde aparece el estado
                radicados = temp_Est[i].loc[[st]]['Radicado']

                # Procesos del estado "st" o días del estado "st"
                val = temp_Est[i].loc[[st]][v].astype(float) 
                # redondeo
                if v == 'Dias estado':
                    val = np.round(val.values,4)
                else:
                    val = val.values 
                
                # Máximo y peor radicado en el periodo
                idx_max = np.argmax(val)
                peorRadicado = radicados[idx_max]
                valMax = val[idx_max]
                
                # Historial 
                df_hist = pd.DataFrame(hist_data) 
                if bool(hist_data):
                    if st in df_hist['Nombre'].values:
                    
                        df_hist = df_hist.set_index("Nombre")
                        CounterHist = collections.Counter(df_hist.loc[st]["Variables"][i])
                        val = [float(x) for x in list(CounterHist.elements())] + list(val)

                        if len(val) <= Nmuestras: # Mantener las últimas n muestras para el análisis
                            strVal = [str(x) for x in val]
                            CounterNew = collections.Counter(strVal)
                            var.append(CounterNew)
                        else: 
                            val = val[-Nmuestras:]
                            strVal = [str(x) for x in val]
                            CounterNew = collections.Counter(strVal)
                            var.append(CounterNew)
                    else:
                        strVal = [str(x) for x in val]
                        CounterNew = collections.Counter(strVal)
                        var.append(CounterNew)
                else: 
                    strVal = [str(x) for x in val]
                    CounterNew = collections.Counter(strVal)
                    var.append(CounterNew)
                
                # Outliers
                cant_outliers, th = find_outliers_IQR(val) # cantidad de radicados atípicos, threshold, valor máximo 
                
                result['ColeccionLog'] = cole
                result['Proyecto'] = proj
                result['Proceso'] = proc
                result['TipoAnalisis'] = 'Estado'
                result['Nombre'] = st
                result['Servicio'] = service_dict[st]
                result['Metrica'] = v
                result['FechaCreacion'] = datetime.utcnow()
                result['Umbral'] = float(round(th,2)) 
                result['TotalRadicadosPeriodo'] = int(total_rad_periodo)
                result['TotalRadicados'] = int(len(radicados.unique()))             
                result['RadicadosSobreUmbral'] = int(cant_outliers)
                result['PorcentajeRadicadosSobreUmbral'] = float(round(cant_outliers/len(radicados.unique())*100,2))
                result['PeorRadicado'] = peorRadicado
                result['ValorMetricaPeorRadicado'] = float(round(valMax,2))
                result['DiferenciaRadicadosInOut'] = int(dif)
                results.append(result)

            hist['Variables'] = var        
            historial.append(hist)

    
    # ANÁLISIS POR COMBINACIÓN DE ESTADOS
    variables = ['Procesos combinacion estado']
    # type_alert = ['Reprocesos por combinacion de estados']
    for idx, cst in enumerate(comb_estado):
        var = []
        hist = {}
        if cst in df['Combinacion estado'].values:
            hist['Nombre'] = cst
            for i, v in enumerate(variables):
                result = {}

                # Radicados donde aparece comb_st
                radicados = temp_Comb.loc[[cst]]['Radicado']

                # Procesos de comb_estado "cst"
                val = temp_Comb.loc[[cst]][v].astype(float).values 

                # Máximo y peor radicado en el periodo
                idx_max = np.argmax(val)
                peorRadicado = radicados[idx_max]
                valMax = val[idx_max]
                
                # Historial 
                df_hist = pd.DataFrame(hist_data) 
                if bool(hist_data):
                    if cst in df_hist['Nombre'].values:
                    
                        df_hist = df_hist.set_index("Nombre")
                        CounterHist = collections.Counter(df_hist.loc[cst]["Variables"][i])
                        val = [float(x) for x in list(CounterHist.elements())] + list(val)

                        if len(val) <= Nmuestras: # Mantener las últimas n muestras para el análisis
                            strVal = [str(x) for x in val]
                            CounterNew = collections.Counter(strVal)
                            var.append(CounterNew)
                        else: 
                            val = val[-Nmuestras:]
                            strVal = [str(x) for x in val]
                            CounterNew = collections.Counter(strVal)
                            var.append(CounterNew)
                    else:
                        strVal = [str(x) for x in val]
                        CounterNew = collections.Counter(strVal)
                        var.append(CounterNew)
                else: 
                    strVal = [str(x) for x in val]
                    CounterNew = collections.Counter(strVal)
                    var.append(CounterNew)
                
                # Outliers
                cant_outliers, th = find_outliers_IQR(val) 

                result['ColeccionLog'] = cole
                result['Proyecto'] = proj
                result['Proceso'] = proc
                result['TipoAnalisis'] = 'Combinacion de estados'
                result['Nombre'] = cst
                result['Servicio'] = service_dict[df['Estado'][idx]] + '-' +service_dict[df['Estado Destino'][idx]]
                result['Metrica'] = v
                result['FechaCreacion'] = datetime.utcnow()
                result['Umbral'] = float(round(th,2)) 
                result['TotalRadicadosPeriodo'] = int(total_rad_periodo)
                result['TotalRadicados'] = int(len(radicados.unique()))             
                result['RadicadosSobreUmbral'] = int(cant_outliers)
                result['PorcentajeRadicadosSobreUmbral'] = float(round(cant_outliers/len(radicados.unique())*100,2))
                result['PeorRadicado'] = peorRadicado
                result['ValorMetricaPeorRadicado'] = float(round(valMax,2))
                results.append(result)

            hist['Variables'] = var        
            historial.append(hist)      

    # ANÁLISIS POR RADICADO
    var = []
    hist = {}
    hist['Nombre'] = 'No aplica'
    variables = ['Veces radicado','Estados radicado','Dias radicado']
    # type_alert = ['Veces radicado','Estados radicado','Dias laborados por radicado']
    for i, v in enumerate(variables):
        result = {}
        # Procesos radicado, estados radicado o dias radicado 
        val = temp_Rad[i][v].astype(float) 
        # redondeo
        if v == 'Dias radicado':
            val = np.round(val.values,4)
        else:
            val = val.values

        # Máximo y peor radicado en el periodo
        idx_max = np.argmax(val)
        peorRadicado = temp_Rad[i]['Radicado'][idx_max]
        valMax = val[idx_max]


        # Historial 
        df_hist = pd.DataFrame(hist_data) 
        if bool(hist_data):
            if 'No aplica' in df_hist['Nombre'].values:
                    
                df_hist = df_hist.set_index("Nombre")
                CounterHist = collections.Counter(df_hist.loc['No aplica']['Variables'][i])
                val = [float(x) for x in list(CounterHist.elements())] + list(val)

                if len(val) <= Nmuestras: # Mantener las últimas n muestras para el análisis
                    strVal = [str(x) for x in val]
                    CounterNew = collections.Counter(strVal)
                    var.append(CounterNew)
                else: 
                    val = val[-Nmuestras:]
                    strVal = [str(x) for x in val]
                    CounterNew = collections.Counter(strVal)
                    var.append(CounterNew)
            else:
                strVal = [str(x) for x in val]
                CounterNew = collections.Counter(strVal)
                var.append(CounterNew)
        else: 
            strVal = [str(x) for x in val]
            CounterNew = collections.Counter(strVal)
            var.append(CounterNew)
        
        # Outliers
        cant_outliers, th = find_outliers_IQR(val) 
        idx_max = np.argmax(val)

        result['ColeccionLog'] = cole
        result['Proyecto'] = proj
        result['Proceso'] = proc
        result['Nombre'] = 'No aplica'
        result['TipoAnalisis'] = 'Radicado'
        result['Metrica'] = v
        result['FechaCreacion'] = datetime.utcnow()
        result['Umbral'] = float(round(th,2)) 
        result['TotalRadicadosPeriodo'] = int(total_rad_periodo)
        result['TotalRadicados'] = int(total_rad_periodo)             
        result['RadicadosSobreUmbral'] = int(cant_outliers)
        result['PorcentajeRadicadosSobreUmbral'] = float(round(cant_outliers/total_rad_periodo*100,2))
        result['PeorRadicado'] = peorRadicado
        result['ValorMetricaPeorRadicado'] = float(round(valMax,2))
        results.append(result)      
        
    hist['Variables'] = var        
    historial.append(hist) 

    return results, historial


