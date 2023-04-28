import numpy as np
import pandas as pd
import collections
from datetime import datetime

from alarms.utils import find_outliers_IQR

class Alerts:
    def __init__(self, data, histData, typeAnalysis, variables, temp, difReg, config, totalRadPeriodo):
        self.data = data 
        self.histData = histData
        self.typeAnalysis = typeAnalysis
        self.variables = variables
        self.temp = temp
        self.difReg = difReg
        self.config = config
        self.totalRadPeriodo = totalRadPeriodo
       
    def analysis(self, name, service_dict=None, radsIn=None, radsOut=None, idx=None):
        historial = []
        results = []
        var = []
        hist = {}
        Nmuestras = int(2E6)
        if name in self.data[self.typeAnalysis].values or self.typeAnalysis == 'Radicado':
            hist['Nombre'] = name
            
            if self.typeAnalysis == 'Estado':
                # Diferencia radicados in/out en st
                if (name in radsIn.index) and (name in radsOut.index):
                    Rin = radsIn.loc[[name]].item()
                    Rout = radsOut.loc[[name]].item()
                    dif = Rin - Rout
                else:
                    dif = 0
            else: 
                dif = None
            
            for i, v in enumerate(self.variables):
                result = {}
                
                if self.typeAnalysis == "Radicado":
                    # Procesos radicado, estados radicado o dias radicado 
                    valPer = self.temp[i][v].astype(float).values
                    # peor radicado en el periodo
                    peorRadicado = self.temp[i]['Radicado'][np.argmax(valPer)]
                else: 
                    # Radicados donde aparece el estado
                    radicados = self.temp[i].loc[[name]]['Radicado']
                    # Procesos del estado "st" o días del estado "st"
                    valPer = self.temp[i].loc[[name]][v].astype(float).values  
                    peorRadicado = radicados[np.argmax(valPer)]

                # valor máximo
                valMax = valPer[np.argmax(valPer)]

                # Umbral periodo
                thPer = find_outliers_IQR(valPer)
    
                # Historial 
                df_hist = pd.DataFrame(self.histData) 
                if bool(self.histData):
                    if name in df_hist['Nombre'].values:
                    
                        df_hist = df_hist.set_index("Nombre")
                        CounterHist = collections.Counter(df_hist.loc[name]["Variables"][i])
                        if self.difReg is not None:
                            tempDif = self.temp[i][-self.difReg:]
                            if name in tempDif.index:
                                valDif = tempDif.loc[[name]][v].astype(float).values
                                valHist = [float(x) for x in list(CounterHist.elements())] + list(valDif)
                            else: 
                                valHist = [float(x) for x in list(CounterHist.elements())]
                        else:
                            valHist = [float(x) for x in list(CounterHist.elements())] + list(valPer)

                        if len(valHist) <= Nmuestras: # Mantener las últimas n muestras para el análisis
                            strVal = [str(x) for x in valHist]
                            CounterNew = collections.Counter(strVal)
                            var.append(CounterNew)
                        else: 
                            valHist = valHist[-Nmuestras:]
                            strVal = [str(x) for x in valHist]
                            CounterNew = collections.Counter(strVal)
                            var.append(CounterNew)

                        # Outliers
                        thHist = find_outliers_IQR(valHist) 
                    else:
                        strVal = [str(x) for x in valPer]
                        CounterNew = collections.Counter(strVal)
                        var.append(CounterNew)
                        # Outliers
                        thHist = find_outliers_IQR(valPer)
                else: 
                    strVal = [str(x) for x in valPer]
                    CounterNew = collections.Counter(strVal)
                    var.append(CounterNew)
                    # Outliers
                    thHist = find_outliers_IQR(valPer)
                
                # cantidad de valores que superan el umbral del historial
                cant_outliers = (valPer > thHist).sum()

                result['ColeccionLog'] = self.config['ColeccionLogs']
                result['Proyecto'] = self.config['Proyecto']
                result['Proceso'] = self.config['Proceso']
                result['Nombre'] = name
                if service_dict is not None and self.typeAnalysis == "Estado":
                    result['Servicio'] = service_dict[name]
                elif service_dict is not None and self.typeAnalysis == 'Combinacion estado':
                    result['Servicio'] = service_dict[self.data['Estado'][idx]] + '-' +service_dict[self.data['Estado Destino'][idx]]
                else: 
                    result['Servicio'] = None
                result['TipoAnalisis'] = self.typeAnalysis
                result['Metrica'] = v
                result['FechaCreacion'] = datetime.utcnow()
                result['UmbralHistorial'] = float(round(thHist,6)) 
                result['UmbralPeriodo'] = float(round(thPer,6)) 
                result['TotalRadicadosPeriodo'] = int(self.totalRadPeriodo)
                if self.typeAnalysis == "Radicado":
                    result['TotalRadicadosEstado'] = int(self.totalRadPeriodo)
                else:
                    result['TotalRadicadosEstado'] = int(len(radicados.unique()))             
                result['RadicadosSobreUmbralHistorico'] = int(cant_outliers) #####
                result['PorcentajeRadicadosSobreUmbralHistorico'] = float(round(result['RadicadosSobreUmbralHistorico']/result['TotalRadicadosEstado']*100,2))
                result['RadicadoPeorMetrica'] = peorRadicado
                result['ValorMetricaPeorRadicado'] = float(round(valMax,2))
                result['VecesSalidas_vs_Ingresos'] = dif
                results.append(result)

            hist['Variables'] = var        
            historial.append(hist)
        return results, historial



