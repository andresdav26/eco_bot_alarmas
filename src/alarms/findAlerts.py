import numpy as np


class byEstados: 
    def __init__(self, df, estados, config, difReg=None):
        self.df = df
        self.estados = estados
        self.config = config
        self.difReg = difReg
        self.total_rad_periodo = len(df['Radicado'].unique()) # Total Radicados en el periodo
        self.estados = set(df['Estado'].unique().tolist() + df['Estado Destino'].unique().tolist()) # Estados
        self.comb_estado = df['Combinacion estado'].unique() # Combinación de estados

    def find_outliers_IQR(self, val):
        q75, q25  = np.percentile(val, [75, 25])
        IQR = q75 - q25
        th = q75 + 1.5*IQR                                        
        return th
        

    def results(self):
        pass