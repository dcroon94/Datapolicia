#importeer paketten
import pandas as pd
import numpy as np
from get_query import get_new_messages

## GET DATA
def insert_new_messages_in_db():
    df = get_new_messages()

    #als er nieuwe berichten zijn opgehaald voor het volgende uit, doe anders niks:
    if df.shape[0] != 0:
        ## DATA PREPPING

        #haal alle datatypes op, alle zijn objecten
        df.dtypes

        # KOLOMNAMEN
        #aanpassen kolomnamen van '-' naar '_'
        df.columns = df.columns.str.replace('-', '_')
    
        # NA'S TOEWIJZEN EN LEGE VARIABELEN VERWIJDEREN
        #vervang alle lege waardes "" met daadwerkelijke NaN waardes
        df = df.replace("", np.NaN)
        #verwijder alle variabelen die volledig gevuld met NA's zijn
        df = df.dropna(how='all', axis=1)

        # CONVERTEER TYPES EN VERWIJDER OVERBODIGE TYPES
        #converteer de typen van object naar het juiste format
        df = df.convert_dtypes()
        #Zet alle string datum om naar datetime types (alle waardesm met datum delict hebben het format YYYY-mm-dd
        df.loc[:,df.columns.str.contains("datumdelict")] = df.filter(regex="datumdelict").apply(pd.to_datetime, format="%Y-%m-%d")
        #Publicatie datum toont ook de uren, minuten en seconden
        df["publicatiedatum"] = pd.to_datetime(df["publicatiedatum"], format="%Y-%m-%d %H:%M:%S")
        #pas locaties (een nested list) aan naar string waarde
        df['locaties']= df['locaties'].astype('str')
        #verwijder alle object types van de dataset
        df = df.select_dtypes(exclude='object')
        # alternatief: df.loc[:,list(df.dtypes != "object")]

        #moeten we deze nog toekennen via bv. SQL alchemy?

        ## LOAD DATA TO POSTGRESSQL DATABASE

        #Methode 1: via SQL alchemy engine
        #engine = create_engine('postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require', echo=True)
        #df.to_sql('crime_api', con=engine, if_exists='replace')
        #engine.execute("SELECT * FROM crime_api").fetchall()

        #Methode 2: direct via pandas to_sql
        df.to_sql(name='crime_api',
                         con='postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require',
                         if_exists='append')  #of 'fail', append
                         #databasetype://admin:wachtwoord@host:port/naam_database

        print(f"{len(df)} nieuwe berichten toegevoegd")
    else:
        print("Geen nieuwe berichten")
    return "finished"

