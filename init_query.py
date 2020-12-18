#laad de gewenste pakketen in
import requests #voor het binnen halen van de API data
import pandas as pd #importeren van pandas
from pandas import json_normalize #normaliseren van JSON data naar een flat pandas file

#documentatie van de politie API:
# https://www.politie.nl/binaries/content/assets/politie/algemeen/algemeen/politieapi.pdf

#opzetten van de API
def get_init_data():
    #de URL waar de data staat
    api_url = "https://api.politie.nl/v4/gezocht"
    #de parameters voor de request, initialiseer offset met 0.
    params = {"language": "nl",
              "maxnumberofitems": "25",
              "offset": 0} #kan enkel een vermenigvuldiging zijn van het aantal items

    #initialisatie van variabelen
    df_policia = pd.DataFrame() #creeer een lege dataframe
    offset = 0 #initialiseer de offset op 0
    # initialiseer een lege lijst voor de locaties
    loc_list = []

    #draai een while loop totdat de query geen resultaten meer vindt, stop dan de loop.
    while True:

        ##OFFSET
        #vervang de offset parameter met een vermenigvuldiging van 25, vang aan met 0 en dan per loop + 25.
        params["offset"] = offset
        ##REQUEST
        #voer de request uit met de gegeven parameters en offset
        response = requests.get(url=api_url, params=params)

        #Als berichten 'out of bound' zijn, break dan de loop (alle berichten zijn opggehaald): No results found
        if response.status_code == 204:
             break

        #als dat niet het geval (resultaten zijn gevonden), dan:

        #zet de response call om naar een json
        json_berichten = response.json()
        #normaliseer het json bericht en converteer naar een pandas dataframe van deze set berichten.
        df_unnested_berichten = json_normalize(json_berichten["opsporingsberichten"], sep='_')

        ##LOCATIES
        #verkrijg de coordinaten (locatie) bij alle verdachte incidenten.

        #loop langs het aantal berichten in de laatste opgehaalde API query en zet alle coordinaten om tot een np.array
        #dit is nodig omdat een incident meerdere locaties kan hebben en dus niet een keer kan worden omgezet. Het aantal
        #berichten is in principe 25 alleen bij de laatste loop is dit mogelijk kleiner dan 25.

        for i in range(len(df_unnested_berichten)):
            #normaliseer elke locaties, dit zet het automatisch om van een list met een dictionary naar een np.array
            loc = json_normalize(json_berichten["opsporingsberichten"][i], record_path="locaties")
            #of: pd.DataFrame(json_berichten["opsporingsberichten"][10]["locaties"]).values

            #voeg de np.array met coordinaten toe aan de lijst met locaties
            loc_list.append(loc.values)
        #herhaal dit voor alle berichten

        #wijs deze lijst aan arrays met coordinaten toe aan de dataset van deze set berichten

        #voeg de dataframe van deze set  berichten toe aan de policia dataset
        df_policia = df_policia.append(df_unnested_berichten)
        print("aantal rijen in dataframe:", len(df_policia))

        # tel voor de volgende loop 25 bij de offset op.
        offset += 25

    df_policia["locaties"] = loc_list

    return df_policia



#samenvatting van de opgehaalde data
# describe_data = df_policia.describe(include="all").transpose()

# #krijg een waardevolle foutmelding als de request niet lukt, als alles goed is dan dient de request naar json
# elif response.status_code == 400:
# raise TypeError("Invalid parameter, vervang de parameters")
# wat als foute URL error?
