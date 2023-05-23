import json
from urllib import request as req

class TeamsWebhookException(Exception):
    """custom exception for failed webhook call"""
    pass


class ConnectorCard:
    def __init__(self, payload:str, hookurl:str):
        self.payload = payload
        self.hookurl = hookurl

    def editpayload(self, typealert, project, process, name, period):
        self.payload['sections'][0]['activityTitle'] = "ALERTA POR: " + typealert
        self.payload['sections'][0]['facts'][0]['value'] = project
        self.payload['sections'][0]['facts'][1]['value'] = process
        self.payload['sections'][0]['facts'][2]['value'] = name
        self.payload['sections'][0]['facts'][3]['value'] = period
        return self

    def post_message(self) -> None:
        request = req.Request(url=self.hookurl, method="POST",headers={'User-Agent':'Python-urllib/3.10'})
        request.add_header(key="Content-Type", val="application/json")

        data = json.dumps(self.payload).encode()
        with req.urlopen(url=request, data=data) as response:
            if response.status != 200:
                raise TeamsWebhookException(response.reason)

# if __name__ == "__main__":
    
#     URL_WEBHOOK = os.environ["WEBHOOK"]

#     # load card .json
#     card_path = Path("/home/adguerrero/Documents/eco_bot_alarmas/src/alarm_module/card.json")
#     with open(card_path, "rb") as in_file:
#         datacard = json.load(in_file)

#     connect = ConnectorCard(datacard,URL_WEBHOOK)
#     connect.editpayload("Reprocesos estado", "IUVA", "NOVEDADES DE ASESORIA", "NOVEDAD DISPONIBLE EN SEGUNDA AUDITORIA","202201")
#     connect.post_message()