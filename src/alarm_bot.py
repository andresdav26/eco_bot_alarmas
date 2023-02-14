import os
from datetime import datetime
import logging
import sys
from pymongo import MongoClient
from mongodb import fetch_data
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

        logger.info(f'Actualizando {configs[0]["ColeccionLogs"]} {configs[0]["Proyecto"]}...'.replace('None', ''))
        # ========= alarmas process ========
        logs_data = fetch_data(
            collection=config_to_update['ColeccionLogs'],
            period=f'{now.year}{now.month:02d}',
            conn=MONGODB_CONN_LOGS
            )
        if bool(logs_data):
            logger.info(f'Procesando {len(logs_data)} registros de logs.')
            alarms = find_alerts(logs_data)

            logger.info(f'{len(alarms)} alarmas generadas.')
            update_end = datetime.utcnow()

            for alarm in alarms:
                alarm['ColeccionLog'] = config_to_update['ColeccionLogs']
                alarm['Proyecto'] = config_to_update['Proyecto']
                alarm['Proceso'] = config_to_update['Proceso']
                alarm['Periodo'] = f'{now.year}{now.month:02d}'
                alarm['UltimaActualizacion'] = update_end

                alarms_collection.update_one(
                    filter={'$and': [
                        {'Tipo_de_analisis': alarm['Tipo_de_analisis']},
                        {'Nombre': alarm['Nombre']},
                        {'Periodo': alarm['Periodo']},
                        {'ColeccionLog': alarm['ColeccionLog']},
                        {'Proyecto': alarm['Proyecto']},
                        {'Proceso': alarm['Proceso']},
                        ]},
                    update={'$set': alarm},
                    upsert=True
                )

        else:
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
