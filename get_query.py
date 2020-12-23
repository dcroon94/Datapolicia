import pandas as pd
import requests
from pandas import json_normalize  # normaliseren van JSON data naar een flat pandas file
import numpy as np
from sqlalchemy import create_engine

def get_new_messages():
    # query die laatste regel ophaalt uit de dataset
    sql_query = "SELECT * FROM crime_api \
                ORDER BY INDEX DESC \
                LIMIT 1"

    engine = create_engine('postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require', echo=True)
    # voer de query uit
    last_row = pd.read_sql(sql_query, con=engine)

    #verkrijg de index
    index_next_row = last_row["index"][0] + 1
    print(last_row["uid"])

    # url om bericht uit politie api op te halen
    api_url = "https://api.politie.nl/v4/gezocht"

    # de parameters voor de request, initialiseer offset met 0.
    params = {"language": "nl",
              "maxnumberofitems": "10"}  # minimum aantal is 10

    # initialiseer ofsett
    offset = 0

    # initialiseren van dataframe en lijst om locaties in te zetten
    df = pd.DataFrame()
    loc_list = []

    # begin een loop totdat deze een break krijgt als bericht overeenkomt met UID van laatste bericht uit db.
    while True:

        ##OFFSET
        # vervang de offset parameter met een vermenigvuldiging van 25, vang aan met 0 en dan per loop + 25.
        params["offset"] = offset

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
            print("bericht", bericht['uid'])
            print("laatste rij", last_row['uid'][0])

            # controleer of bericht niet overeenkomt met uid laatse bericht db, breek anders uit de loop
            if bericht['uid'] == last_row['uid'][0]:
                break

            # normaliseer bericht N
            new_bericht = json_normalize(bericht, sep='_')

            # voeg bericht N toe aan dataframe
            df = df.append(new_bericht)

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
    df["locaties"] = loc_list

    #wijs index toe en sorteer, eerst bericht als laatst.
    df.index = np.array(range(index_next_row + len(df), index_next_row, -1))
    df = df.sort_index()

    #sluit tot slot de engine weer
    engine.dispose()

    return df
