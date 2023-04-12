from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd

def fetch_data(
    collection: str,
    project: str,
    process: str,
    conn: str,
    period: str,
    nowDate: datetime | None = None,
    extra_fields: dict | None = None
    ) -> list:
    """
    Query log data from the database.

    Args:
        collection (str): collection name.
        project (str): project name. 
        process (str): process name. 
        period (str): time period (year-month) in yyyymm format. Manual analysis.
        nowDate (datetime | None, optional): Current date. Defaults to None.
        extra_fields (dict | None, optional): Metadata. Defaults to None.

    Returns:
        list: requested data.
    """


    client = MongoClient(conn)
    database = client.ebk_logs

    documents = []

    query = {}
    query['Proyecto'] = project
    query['Proceso'] = process

    if nowDate is not None:
        initDate = (nowDate - timedelta(days=30)).replace(hour=0,minute=0,second=0,microsecond=0) # 30 días desde la fecha actual. 
        cursortemp = database[collection].find(
                    {'Fecha Inicio / Hora':{'$lt': nowDate, '$gte': initDate}},  # fecha final, fecha inicial
                    projection = {'_id': 0, 'Periodo': 1}
                    )
        listPer = []
        for document in cursortemp:
            listPer.append(document['Periodo'])

        listPer = list(set(listPer))
        query['Periodo'] = {"$in":listPer} # todos los cambios de estado para los radicados de los periodos "listPer".
    else: 
        listPer = [period]
        query['Periodo'] = period # en caso de análsis manaul por periodo. 
        
    projection = {
        '_id': 0,
        'Periodo':1,
        'Proyecto':1,
        'Proceso':1,
        'Radicado': 1,
        'Servicio': 1,
        'Estado': 1,
        'Fecha Inicio / Hora': 1,
        'Fecha Fin / Hora': 1,
        'Estado Destino': 1
        }

    # extra fields
    if bool(extra_fields):
        for field, value in extra_fields.items():
            if not isinstance(value, list):
                query[field] = {'$in': [value]}
            else:
                query[field] = {'$in': value}

        # fist get cases ids
        case_ids = []
        cursor = database[collection].find(
            query,
            projection = {'_id': 0, 'Radicado': 1}
        )

        for doc in cursor:
            case_ids.append(doc)

        case_ids = list(set([d['Radicado'] for d in case_ids]))[:100_000]   # max cases

        cursor = database[collection].find(
            {'Radicado': {'$in': case_ids}},
            projection = projection
        )

    else:
        cursor = database[collection].find(
            query,
            projection=projection
        )

    cursor.limit(1_500_000)

    for document in cursor:
        documents.append(document)

    return documents, listPer


def fetch_data_hist(
    collection: str,
    ) -> list:
    """
    Query log data from the database.

    Args:
        collection (str): collection name.

    Returns:
        list: requested data.
    """

    documents = []

    projection = {
        '_id': 0,
        'ColeccionLog':1,
        'Proyecto':1,
        'Proceso':1,
        'Nombre': 1,
        'Variables': 1,
        }

    cursor = collection.find(
        projection=projection
    )

    cursor.limit(1_500_000)

    for document in cursor:
        documents.append(document)

    return documents
