import os
import time
from datetime import datetime
import logging
import sys
from pymongo import MongoClient
from mongodb import fetch_data, fetch_data_hist
from alarms_process import find_alerts

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
cons_handler = logging.StreamHandler(sys.stdout)
cons_handler.setLevel(logging.INFO)
cons_handler.setFormatter(logging.Formatter('%(name)s [%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(cons_handler)
logger.propagate = False

if 'MONGO_EBKIA_DES' in os.environ:
    MONGODB_CONN_IA = os.environ['MONGO_EBKIA_DES']
    IA_DATABASE = 'ebk_IA_des'
else:
    MONGODB_CONN_IA = os.environ['MONGO_EBKIA_PRU']
    IA_DATABASE = 'ebk_IA_pru'

MONGODB_CONN_LOGS = os.environ['MONGO_LOGS_PRD']

def auto_alarms():
    client = MongoClient(MONGODB_CONN_IA)

    db = client[IA_DATABASE]
    config_collection = db['ConfigAutoalarmas']
    alarms_collection = db['Autoalarmas']
    hist_collection = db['HistAutoalarmas']

    # get active alarm configurations that are not being updated
    configs = [conf for conf in config_collection.find({'Activa': True, 'Actualizando': False})]
    now = datetime.utcnow()

    for conf in configs:
        conf['SecondsSinceUpdate'] = (now - conf['UltimaActualizacion']).total_seconds()
        if conf['SecondsSinceUpdate'] >= conf['FrecuenciaMinutos'] * 60:
            conf['Update'] = True
        else:
            conf['Update'] = False

    # Sort configurations by the time since it's last update
    configs = sorted([conf for conf in configs if conf['Update'] == True], key=lambda x: x['SecondsSinceUpdate'], reverse=True)

    if bool(configs):
        config_to_update = configs[0]
        
        # change updating state
        config_collection.update_one(
            filter={'_id': config_to_update['_id']},
            update={'$set':{'Actualizando': True}}
        )

        logger.info(f'Actualizando {configs[0]["ColeccionLogs"]} {configs[0]["Proceso"]}...'.replace('None', ''))
        
        # ========= alarmas process ========
        periodo = f'{now.year}{now.month:02d}'
        collection=config_to_update['ColeccionLogs']
        # periodo = "202303"
        logs_data = fetch_data(
            collection=collection,
            period=periodo,
            conn=MONGODB_CONN_LOGS
            )
        hist_data = fetch_data_hist(
            collection=hist_collection,
            )

        if bool(logs_data):
            
            # only historial data that match with ColeccionLog, Proyecto and Proceso 
            hist_data = [p for p in hist_collection.find({"ColeccionLog":config_to_update["ColeccionLogs"],
                                                          "Proyecto":config_to_update["Proyecto"], 
                                                          "Proceso":config_to_update["Proceso"]})]

            logger.info(f'Procesando {len(logs_data)} registros de logs en el periodo {periodo}')

            condPer = [p for p in config_collection.find({'_id': config_to_update['_id'],'PeriodosConsultados': periodo})]
            if bool(condPer): 
                idx = condPer[0]['PeriodosConsultados'].index(periodo)
                OldNumReg = condPer[0]['CantRegistros'][idx]

                difReg = len(logs_data) - OldNumReg # Difference between old and new registers number by a period 
                if difReg > 0:
                    # updating periods - registers 
                    config_collection.update_one(
                        filter={'_id': config_to_update['_id']},
                        update={"$set": {"CantRegistros." + str(idx): OldNumReg+difReg}},
                        )
                    logger.info(f'Existen {difReg} registros nuevos.')
                    # Alarms
                    t0 = time.time()
                    alarms, historial = find_alerts(logs_data, hist_data, config_to_update, difReg)
                    logger.info(f'Tiempo: {round(time.time()-t0,2)} seg.')
                else:
                    logger.info(f'No existen registros nuevos.')
                    config_collection.update_one(
                        filter={'_id': config_to_update['_id']},
                        update={'$set': {'Actualizando': False, 'UltimaActualizacion': datetime.utcnow()}}
                        )
                    return auto_alarms()
            else:
                # updating periods - registers 
                config_collection.update_one(
                    filter={'_id': config_to_update['_id']},
                    update={"$addToSet": {'PeriodosConsultados': periodo, "CantRegistros": len(logs_data)}},
                )
                # Alarms
                t0 = time.time()
                alarms, historial = find_alerts(logs_data, hist_data, config_to_update)
                logger.info(f'Tiempo: {round(time.time()-t0,2)} seg.')

            update_end = datetime.utcnow()

            for hist in historial:
                hist['ColeccionLog'] = config_to_update['ColeccionLogs']
                hist['Proyecto'] = config_to_update['Proyecto']
                hist['Proceso'] = config_to_update['Proceso']

                hist_collection.update_one(
                    filter={'$and': [
                        {'ColeccionLog': hist['ColeccionLog']},
                        {'Proyecto': hist['Proyecto']},
                        {'Proceso': hist['Proceso']},
                        {'Nombre': hist['Nombre']},
                        ]},
                    update={'$set': hist},
                    upsert=True
                )

            for alarm in alarms:
                alarm['Periodo'] = periodo
                alarms_collection.insert_one(alarm)

        else:
            update_end = datetime.utcnow()
            logger.warning(f'No se obtubieron datos de logs.')
        # =========================
        config_collection.update_one(
            filter={'_id': config_to_update['_id']},
            update={'$set': {'Actualizando': False, 'UltimaActualizacion': update_end}}
        )
    else:
        pass

    client.close()

    return None

if __name__ == '__main__':
    auto_alarms()
