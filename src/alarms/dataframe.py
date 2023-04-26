from datetime import timedelta
from utils import calculate_times

import pandas as pd

class Data:
    def __init__(self, df):
        self.df = df
        self.data = []
     
    def preprocess(self, df:pd.DataFrame) -> pd.DataFrame:
        df = df.reset_index(drop=True)
        df['Radicado'] = df['Radicado'].astype(str)
        df['Combinacion estado'] = df['Estado']+'-'+df['Estado Destino'] 

        df['Fecha Inicio / Hora'] = df['Fecha Inicio / Hora'] - timedelta(hours=5)
        df['Fecha Fin / Hora'] = df['Fecha Fin / Hora'] - timedelta(hours=5)

        # Serivicios 
        df_serv1 = df[['Estado', 'Servicio']].drop_duplicates().rename(columns={'Estado': 'estado'})
        df_serv2 = df[['Estado Destino', 'Servicio']].drop_duplicates().rename(columns={'Estado Destino': 'estado'})
        df_serv = pd.concat([df_serv1, df_serv2], ignore_index=True).drop_duplicates(subset='estado')
        service_dict = {row['estado']: row['Servicio'] for _, row in df_serv.iterrows()}

        # Días laborados por registro
        df['tiempo_estado'] = calculate_times(df)
        return df

    def DataEstado(self, df:pd.DataFrame) -> list():
        data = self.data
        data.append(df.groupby(by=["Radicado","Estado"])["Estado"].count().reset_index(0).rename(columns={'Estado':'Reprocesos estado'}), # procesos por estados 
                    df.groupby(by=['Radicado', 'Estado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Días estado'}).round(4)) # días por estado
        return data 
    
    def DataCombEstado(self, df:pd.DataFrame) -> list():
        data = self.data
        data.append(df.groupby(by=["Radicado","Combinacion estado"])["Combinacion estado"].count().reset_index(0).rename(columns={'Combinacion estado':'Procesos combinación estado'})) 
        return data
    
    def DataRadicado(self, df:pd.DataFrame) -> list():
        data = self.data
        temp = df.groupby(by=["Radicado","Estado"])["Estado"].count().reset_index(0).rename(columns={'Estado':'Reprocesos estado'})
        data.append(temp.groupby(by=["Radicado"]).sum().reset_index(0).rename(columns={'Reprocesos estado':'Veces radicado'}), # procesos por radicado (sumatoria de los procesos que contiene cada estado)
                    temp.groupby(by=["Radicado"]).count().reset_index(0).rename(columns={'Reprocesos estado':'Estados radicado'}), # cantidad estados por radicado
                    df.groupby(by=['Radicado'])['tiempo_estado'].sum().reset_index(0).rename(columns={'tiempo_estado':'Días radicado'}).round(4))   # días por radicado
        return data
    



# def find_alerts(df, hist_data, config, difReg=None):
    

#     # MAIN 
#     results = []
#     historial = []
#     Nmuestras = int(2E6)

#     # filtros de consulta
#     cole = config['ColeccionLogs']
#     proj = config['Proyecto']
#     proc = config['Proceso']

#     # ANÁLISIS POR ESTADO 
#     variables = ['Reprocesos estado', 'Días estado']
#     rads_in = df.groupby(by=["Estado Destino"])["Radicado"].count()
#     rads_out = df.groupby(by=["Estado"])["Radicado"].count()
#     for st in estados:
#         var = []
#         hist = {}
#         if st in df["Estado"].values:
#             hist['Nombre'] = st
            
#             # Diferencia radicados in/out en st
#             if (st in rads_in.index) and (st in rads_out.index):
#                 Rin = rads_in.loc[[st]].item()
#                 Rout = rads_out.loc[[st]].item()
#                 dif = Rin - Rout
#             else:
#                 dif = 0
            
#             for i, v in enumerate(variables):
#                 result = {}

#                 # Radicados donde aparece el estado
#                 radicados = temp_Est[i].loc[[st]]['Radicado']

#                 # Procesos del estado "st" o días del estado "st"
#                 valPer = temp_Est[i].loc[[st]][v].astype(float).values  
                
#                 # Umbral periodo
#                 thPer = find_outliers_IQR(valPer)

#                 # Máximo y peor radicado en el periodo
#                 idx_max = np.argmax(valPer)
#                 peorRadicado = radicados[idx_max]
#                 valMax = valPer[idx_max]
                
#                 # Historial 
#                 df_hist = pd.DataFrame(hist_data) 
#                 if bool(hist_data):
#                     if st in df_hist['Nombre'].values:
                    
#                         df_hist = df_hist.set_index("Nombre")
#                         CounterHist = collections.Counter(df_hist.loc[st]["Variables"][i])
#                         if difReg is not None:
#                             tempDif = temp_Est[i][-difReg:]
#                             if st in tempDif.index:
#                                 valDif = tempDif.loc[[st]][v].astype(float).values
#                                 valHist = [float(x) for x in list(CounterHist.elements())] + list(valDif)
#                             else: 
#                                 valHist = [float(x) for x in list(CounterHist.elements())]
#                         else:
#                             valHist = [float(x) for x in list(CounterHist.elements())] + list(valPer)

#                         if len(valHist) <= Nmuestras: # Mantener las últimas n muestras para el análisis
#                             strVal = [str(x) for x in valHist]
#                             CounterNew = collections.Counter(strVal)
#                             var.append(CounterNew)
#                         else: 
#                             valHist = valHist[-Nmuestras:]
#                             strVal = [str(x) for x in valHist]
#                             CounterNew = collections.Counter(strVal)
#                             var.append(CounterNew)

#                         # Outliers
#                         thHist = find_outliers_IQR(valHist) 
#                     else:
#                         strVal = [str(x) for x in valPer]
#                         CounterNew = collections.Counter(strVal)
#                         var.append(CounterNew)
#                         # Outliers
#                         thHist = find_outliers_IQR(valPer)
#                 else: 
#                     strVal = [str(x) for x in valPer]
#                     CounterNew = collections.Counter(strVal)
#                     var.append(CounterNew)
#                     # Outliers
#                     thHist = find_outliers_IQR(valPer)
                
#                 # cantidad de valores que superan el umbral del historial
#                 cant_outliers = (valPer > thHist).sum()

#                 result['ColeccionLog'] = cole
#                 result['Proyecto'] = proj
#                 result['Proceso'] = proc
#                 result['Nombre'] = st
#                 result['Servicio'] = service_dict[st]
#                 result['TipoAnalisis'] = 'Estado'
#                 result['Metrica'] = v
#                 result['FechaCreacion'] = datetime.utcnow()
#                 result['UmbralHistorial'] = float(round(thHist,6)) 
#                 result['UmbralPeriodo'] = float(round(thPer,6)) 
#                 result['TotalRadicadosPeriodo'] = int(total_rad_periodo)
#                 result['TotalRadicadosEstado'] = int(len(radicados.unique()))             
#                 result['RadicadosSobreUmbralHistorico'] = int(cant_outliers) #####
#                 result['PorcentajeRadicadosSobreUmbralHistorico'] = float(round(cant_outliers/len(radicados.unique())*100,2))
#                 result['RadicadoPeorMetrica'] = peorRadicado
#                 result['ValorMetricaPeorRadicado'] = float(round(valMax,2))
#                 result['VecesSalidas_vs_Ingresos'] = int(dif)
#                 results.append(result)

#             hist['Variables'] = var        
#             historial.append(hist)

    
#     # ANÁLISIS POR COMBINACIÓN DE ESTADOS
#     variables = ['Procesos combinación estado']
#     # type_alert = ['Reprocesos por combinacion de estados']
#     for idx, cst in enumerate(comb_estado):
#         var = []
#         hist = {}
#         if cst in df['Combinacion estado'].values:
#             hist['Nombre'] = cst
#             for i, v in enumerate(variables):
#                 result = {}

#                 # Radicados donde aparece comb_st
#                 radicados = temp_Comb.loc[[cst]]['Radicado']

#                 # Procesos de comb_estado "cst"
#                 valPer = temp_Comb.loc[[cst]][v].astype(float).values 

#                 # Umbral periodo
#                 thPer = find_outliers_IQR(valPer)

#                 # Máximo y peor radicado en el periodo
#                 idx_max = np.argmax(valPer)
#                 peorRadicado = radicados[idx_max]
#                 valMax = valPer[idx_max]
                
#                 # Historial 
#                 df_hist = pd.DataFrame(hist_data) 
#                 if bool(hist_data):
#                     if cst in df_hist['Nombre'].values:
                    
#                         df_hist = df_hist.set_index("Nombre")
#                         CounterHist = collections.Counter(df_hist.loc[cst]["Variables"][i])
#                         # Si hay registros nuevos solo agrego los nuevos valores
#                         if difReg is not None:
#                             tempDif = temp_Comb[-difReg:]
#                             if cst in tempDif.index:
#                                 valDif = tempDif.loc[[cst]][v].astype(float).values
#                                 valHist = [float(x) for x in list(CounterHist.elements())] + list(valDif)
#                             else: 
#                                 valHist = [float(x) for x in list(CounterHist.elements())]
#                         else:
#                             valHist = [float(x) for x in list(CounterHist.elements())] + list(valPer)

#                         if len(valHist) <= Nmuestras: # Mantener las últimas n muestras para el análisis
#                             strVal = [str(x) for x in valHist]
#                             CounterNew = collections.Counter(strVal)
#                             var.append(CounterNew)
#                         else: 
#                             valHist = valHist[-Nmuestras:]
#                             strVal = [str(x) for x in valHist]
#                             CounterNew = collections.Counter(strVal)
#                             var.append(CounterNew)

#                         # Outliers
#                         thHist = find_outliers_IQR(valHist)
#                     else:
#                         strVal = [str(x) for x in valPer]
#                         CounterNew = collections.Counter(strVal)
#                         var.append(CounterNew)
#                         # Outliers
#                         thHist = find_outliers_IQR(valPer)
#                 else: 
#                     strVal = [str(x) for x in valPer]
#                     CounterNew = collections.Counter(strVal)
#                     var.append(CounterNew)
#                     # Outliers
#                     thHist = find_outliers_IQR(valPer)
                
#                 # cantidad de valores que superan el umbral del historial
#                 cant_outliers = (valPer > thHist).sum()

#                 result['ColeccionLog'] = cole
#                 result['Proyecto'] = proj
#                 result['Proceso'] = proc
#                 result['Nombre'] = cst
#                 result['Servicio'] = service_dict[df['Estado'][idx]] + '-' +service_dict[df['Estado Destino'][idx]]
#                 result['TipoAnalisis'] = 'Combinación de estados'
#                 result['Metrica'] = v
#                 result['FechaCreacion'] = datetime.utcnow()
#                 result['UmbralHistorial'] = float(round(thHist,6))
#                 result['UmbralPeriodo'] = float(round(thPer,6))  
#                 result['TotalRadicadosPeriodo'] = int(total_rad_periodo)
#                 result['TotalRadicadosEstado'] = int(len(radicados.unique()))             
#                 result['RadicadosSobreUmbralHistorico'] = int(cant_outliers)  
#                 result['PorcentajeRadicadosSobreUmbralHistorico'] = float(round(cant_outliers/len(radicados.unique())*100,2))
#                 result['RadicadoPeorMetrica'] = peorRadicado
#                 result['ValorMetricaPeorRadicado'] = float(round(valMax,2))
#                 results.append(result)

#             hist['Variables'] = var        
#             historial.append(hist)      

#     # ANÁLISIS POR RADICADO
#     var = []
#     hist = {}
#     hist['Nombre'] = 'No aplica'
#     variables = ['Veces radicado','Estados radicado','Días radicado']
#     # type_alert = ['Veces radicado','Estados radicado','Dias laborados por radicado']
#     for i, v in enumerate(variables):
#         result = {}
#         # Procesos radicado, estados radicado o dias radicado 
#         valPer = temp_Rad[i][v].astype(float).values
        
#         # Umbral periodo
#         thPer = find_outliers_IQR(valPer)

#         # Máximo y peor radicado en el periodo
#         idx_max = np.argmax(valPer)
#         peorRadicado = temp_Rad[i]['Radicado'][idx_max]
#         valMax = valPer[idx_max]

#         # Historial 
#         df_hist = pd.DataFrame(hist_data) 
#         if bool(hist_data):
#             if 'No aplica' in df_hist['Nombre'].values:
                    
#                 df_hist = df_hist.set_index("Nombre")
#                 CounterHist = collections.Counter(df_hist.loc['No aplica']['Variables'][i])
#                 # Si hay registros nuevos solo agrego los nuevos valores
#                 if difReg is not None:
#                     tempDif = temp_Rad[i][-difReg:]
#                     valDif = tempDif[v].astype(float).values
#                     valHist = [float(x) for x in list(CounterHist.elements())] + list(valDif)
#                 else:
#                     valHist = [float(x) for x in list(CounterHist.elements())] + list(valPer)

#                 if len(valHist) <= Nmuestras: # Mantener las últimas n muestras para el análisis
#                     strVal = [str(x) for x in valHist]
#                     CounterNew = collections.Counter(strVal)
#                     var.append(CounterNew)
#                 else: 
#                     valHist = valHist[-Nmuestras:]
#                     strVal = [str(x) for x in valHist]
#                     CounterNew = collections.Counter(strVal)
#                     var.append(CounterNew)

#                 # Outliers
#                 thHist = find_outliers_IQR(valHist)
#             else:
#                 strVal = [str(x) for x in valPer]
#                 CounterNew = collections.Counter(strVal)
#                 var.append(CounterNew)
#                 # Outliers
#                 thHist = find_outliers_IQR(valPer)
#         else: 
#             strVal = [str(x) for x in valPer]
#             CounterNew = collections.Counter(strVal)
#             var.append(CounterNew)
#             # Outliers
#             thHist = find_outliers_IQR(valPer)

#         # cantidad de valores que superan el umbral del historial
#         cant_outliers = (valPer > thHist).sum() 

#         result['ColeccionLog'] = cole
#         result['Proyecto'] = proj
#         result['Proceso'] = proc
#         result['Nombre'] = 'No aplica'
#         result['TipoAnalisis'] = 'Radicado'
#         result['Metrica'] = v
#         result['FechaCreacion'] = datetime.utcnow()
#         result['UmbralHistorial'] = float(round(thHist,6)) 
#         result['UmbralPeriodo'] = float(round(thPer,6))
#         result['TotalRadicadosPeriodo'] = int(total_rad_periodo)
#         result['TotalRadicadosEstado'] = int(total_rad_periodo)             
#         result['RadicadosSobreUmbralHistorico'] = int(cant_outliers)
#         result['PorcentajeRadicadosSobreUmbralHistorico'] = float(round(cant_outliers/total_rad_periodo*100,2))
#         result['RadicadoPeorMetrica'] = peorRadicado
#         result['ValorMetricaPeorRadicado'] = float(round(valMax,2))
#         results.append(result)      
        
#     hist['Variables'] = var        
#     historial.append(hist) 

#     return results, historial


