#importeer get_init_data uit init_query.py
from init_query import get_init_data
#importeer paketten
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

## GET DATA
#even kijken of het zo moet blijven staan als we gaan werken met airflow of dat het de uitput is van de vorige flow
df_policia = get_init_data()

## DATA PREPPING

#haal alle datatypes op, alle zijn objecten
print(df_policia.dtypes)

# KOLOMNAMEN
#aanpassen kolomnamen van '-' naar '_'
df_policia.columns = df_policia.columns.str.replace('-', '_')

# NA'S TOEWIJZEN EN LEGE VARIABELEN VERWIJDEREN
#vervang alle lege waardes "" met daadwerkelijke NaN waardes
df_policia = df_policia.replace("", np.NaN)
#verwijder alle variabelen die volledig gevuld met NA's zijn
df_policia = df_policia.dropna(how='all', axis=1)

# CONVERTEER TYPES EN VERWIJDER OVERBODIGE TYPES
#converteer de typen van object naar het juiste format
df_policia = df_policia.convert_dtypes()
#Zet alle string datum om naar datetime types (alle waardesm met datum delict hebben het format YYYY-mm-dd
df_policia.loc[:,df_policia.columns.str.contains("datumdelict")] = df_policia.filter(regex="datumdelict").apply(pd.to_datetime, format="%Y-%m-%d")
#Publicatie datum toont ook de uren, minuten en seconden
df_policia["publicatiedatum"] = pd.to_datetime(df_policia["publicatiedatum"], format="%Y-%m-%d %H:%M:%S")
#pas locaties (een nested list) aan naar string waarde
df_policia['locaties']= df_policia['locaties'].astype('str')
#verwijder alle object types van de dataset
df_policia = df_policia.select_dtypes(exclude='object')
# alternatief: df_policia.loc[:,list(df_policia.dtypes != "object")]

# STEL INDEX OPNIEUW IN
#Wijs range met index toe aan de huidige dataset (meest recent: hoogste index, oudst: eerste index)
df_policia.index = np.array(range(df_policia.shape[0], 0, -1))
#sorteer oplopend, zodat deze dus gesorteerd is op basis van wanneer bericht geplaatst is (oudste bericht als eerste)
df_policia = df_policia.sort_index()

#(PRIMARY) KEY kandidaten
df_policia.columns[df_policia.nunique() == df_policia.shape[0]]
df_policia['uid'].is_unique

## LOAD DATA TO POSTGRESSQL DATABASE

#Methode 1: via SQL alchemy engine
#engine = create_engine('postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require', echo=True)
#df_policia.to_sql('crime_api', con=engine, if_exists='replace')
#engine.execute("SELECT * FROM crime_api").fetchall()

#Methode 2: direct via pandas to_sql
df_policia.to_sql(name='crime_test',
                 con='postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require',
                 if_exists='replace')  #of 'fail', append
                 #databasetype://admin:wachtwoord@host:port/naam_database