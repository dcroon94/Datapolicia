import pandas as pd
import requests
from pandas import json_normalize  # normaliseren van JSON data naar een flat pandas file


def get_new_messages():
    # query die laatste regel ophaalt uit de dataset
    sql_query = "SELECT * FROM crime_api \
                ORDER BY INDEX DESC \
                LIMIT 1"

    # voer de query uit
    last_row = pd.read_sql(sql_query,
                           con='postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require')

    # url om bericht uit politie api op te halen
    api_url = "https://api.politie.nl/v4/gezocht"

    # de parameters voor de request, initialiseer offset met 0.
    params = {"language": "nl",
              "maxnumberofitems": "10"}  # minimum aantal is 10

    # initialiseer ofsett
    offset = 0

    # initialiseren van dataframe en lijst om locaties in te zetten
    df_new_messages = pd.DataFrame()
    loc_list = []

    # begin een loop totdat deze een break krijgt als bericht overeenkomt met UID van laatste bericht uit db.
    while True:

        ##OFFSET
        # vervang de offset parameter met een vermenigvuldiging van 25, vang aan met 0 en dan per loop + 25.
        params["offset"] = offset
        print(params)

        ##REQUEST
        # voer de request uit met de gegeven parameters en offset
        response = requests.get(url=api_url, params=params)

        # Als berichten 'out of bound' zijn, break dan de loop (alle berichten zijn opggehaald): No results found
        if response.status_code == 204:
            break

        # als dat niet het geval (resultaten zijn gevonden), dan:

        # zet de response call om naar een json
        json_berichten = response.json()

        # normaliseer per bericht
        for bericht in json_berichten['opsporingsberichten']:

            # controleer of bericht niet overeenkomt met uid laatse bericht db, breek anders uit de loop
            if bericht['uid'] == last_row['uid'][0]:
                break

            # normaliseer bericht N
            new_bericht = json_normalize(bericht, sep='_')

            # voeg bericht N toe aan dataframe
            df_new_messages = df_new_messages.append(new_bericht)

            # normaliseer elke locaties, dit zet het automatisch om van een list met een dictionary naar een np.array
            loc = json_normalize(bericht, record_path="locaties")

            # voeg de np.array met coordinaten toe aan de lijst met locaties
            loc_list.append(loc.values)

        # als bericht overeenkomt met uid laatse bericht db, breek dan ook uit de while loop
        if bericht['uid'] == last_row['uid'][0]:
            break

        #  voor de volgende loop er 10 bij op.
        offset += 10

    # wijs tenslotte de lijs met toe aan de df
    df_new_messages["locaties"] = loc_list

    return df_new_messages
