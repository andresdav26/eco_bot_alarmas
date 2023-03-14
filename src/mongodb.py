from pymongo import MongoClient


def fetch_data(
    collection: str,
    period: str,
    conn: str,
    project: str | None = None,
    process: str | list | None = None,
    extra_fields: dict | None = None
    ) -> list:
    """
    Query log data from the database.

    Args:
        collection (str): collection name.
        period (str | None, optional): time period (year-month) in yyyymm format. Defaults to None.
        project (str | None, optional): project name. Defaults to None.
        process (str | None, optional): process name. Defaults to None.
        date_range (list | None, optional): date range [initial_date, end_date]. Defaults to None.

    Returns:
        list: requested data.
    """

    client = MongoClient(conn)
    database = client.ebk_logs

    documents = []

    query = {}
    if period is not None: query['Periodo'] = period
    if project is not None: query['Proyecto'] = project

    if bool(process):
        if not isinstance(process, list):
            query['Proceso'] = {'$in': [process]}
        else:
            query['Proceso'] = {'$in': process}

    projection = {
        '_id': 0,
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

    return documents


def fetch_data_hist(
    collection: str,
    project: str | None = None,
    process: str | list | None = None,
    ) -> list:
    """
    Query log data from the database.

    Args:
        collection (str): collection name.
        project (str | None, optional): project name. Defaults to None.
        process (str | None, optional): process name. Defaults to None.

    Returns:
        list: requested data.
    """

    documents = []

    query = {}
    if project is not None: query['Proyecto'] = project

    if bool(process):
        if not isinstance(process, list):
            query['Proceso'] = {'$in': [process]}
        else:
            query['Proceso'] = {'$in': process}

    projection = {
        '_id': 0,
        'ColeccionLog':1,
        'Proyecto':1,
        'Proceso':1,
        'Nombre': 1,
        'Variables': 1,
        }

    cursor = collection.find(
        query,
        projection=projection
    )

    cursor.limit(1_500_000)

    for document in cursor:
        documents.append(document)

    return documents
