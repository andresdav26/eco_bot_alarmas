from alarms.preprocess import DataFramepreprocess
from alarms.metric import Metric
from alarms.alerts import Alerts

def find_alerts(df, hist_data, config, difReg=None):
    
    df, service_dict = DataFramepreprocess(df).preprocess()

    temp_Est = Metric(df).metricEstado()
    temp_Comb = Metric(df).metricCombEstado()
    temp_Rad = Metric(df).metricRadicado()

    # variables 
    totalRadPeriodo = len(df['Radicado'].unique()) # Total Radicados en el periodo
    estados = set(df['Estado'].unique().tolist() + df['Estado Destino'].unique().tolist()) # Estados
    comb_estado = df['Combinacion estado'].unique() # Combinación de estados

    # MAIN 
    results = []
    historial = []

    # ANÁLISIS POR ESTADO 
    variables = [temp.columns[-1] for temp in temp_Est]
    rads_in = df.groupby(by=["Estado Destino"])["Radicado"].count()
    rads_out = df.groupby(by=["Estado"])["Radicado"].count()
    alertEstado = Alerts(df, hist_data, "Estado", variables, temp_Est, difReg, config, totalRadPeriodo)
    for st in estados:
        resultEstado, histEstado = alertEstado.analysis(name = st, service_dict=service_dict, radsIn=rads_in, radsOut=rads_out)
        results += resultEstado
        historial  += histEstado
              
    # ANÁLISIS POR COMBINACIÓN DE ESTADOS
    variables = [temp.columns[-1] for temp in temp_Comb]
    alertCombEstado = Alerts(df, hist_data, "Combinacion estado", variables, temp_Comb, difReg, config, totalRadPeriodo)
    for idx, cst in enumerate(comb_estado):
        resultComb, histComb = alertCombEstado.analysis(name = cst, service_dict=service_dict, idx = idx)
        results += resultComb
        historial  += histComb
    
    # ANÁLISIS POR RADICADO
    variables = [temp.columns[-1] for temp in temp_Rad]
    alertRadicado= Alerts(df, hist_data, "Radicado", variables, temp_Rad, difReg, config, totalRadPeriodo)
    resultRadicado, histRadicado = alertRadicado.analysis(name = 'No aplica')
    results += resultRadicado
    historial += histRadicado

    return results, historial


