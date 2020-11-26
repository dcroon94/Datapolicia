import requests
from pandas import json_normalize
import pandas as pd
import numpy as np

##laad politie API in

# https://www.politie.nl/binaries/content/assets/politie/algemeen/algemeen/politieapi.pdf
# request.get("https://api.politie.nl/v4/gezocht?language=nl&lat=53.1511173&lon=6.756634599999984&radius=5.0&maxnumberofitems=10&offset=0")

#de URL waar de data staat
api_url = "https://api.politie.nl/v4/gezocht"
#de parameters voor de request, initialiseer offset met 0.
params = {"language": "nl",
          "maxnumberofitems": "25",
          "offset": 0} #kan enkel een vermenigvuldiging zijn van het aantal items


#loop langs alle offset waardes, plaats deze in de parameters en haal vervolgens de request op.
#kan mogelijk efficienter met een list comprehension
#pas de middelste range waarde aan voor hoeveel data je wilt ophalen.
data = pd.DataFrame()
for offset in range(0, 500, 25):
    #vervang de offset parameter
    params["offset"] = offset

    #voer de request uit
    response = requests.get(url=api_url, params=params)

    #krijg een waardevolle foutmelding als de request niet lukt, als alles goed is dan dient de request naar json
    #te worden omgezet
    if response.status_code == 500:
        raise TypeError("Fout in de request, controleer de URL")
    elif response.status_code == 400:
        raise TypeError("Fout in de request, controleer de parameters")
    else:
        data_json = response.json()
        data_unnested = json_normalize(data_json["opsporingsberichten"], sep='_')

        # verkrijg per verdachte alle coordinaten
        loc_list = []
        for i in range(25):
            loc = json_normalize(data_json["opsporingsberichten"][i],
                                 record_path="locaties")
            #of: pd.DataFrame(data_json["opsporingsberichten"][10]["locaties"]).values
            loc_list.append(loc.values)

        #wijs deze lijst met een array aan coordinaten toe aan de dataset
        data_unnested["locaties"] = loc_list

        #voeg het geheel toe aan de dataset
        data = data.append(data_unnested)

describe_data = data.describe(include="all").transpose()


