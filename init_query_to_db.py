from init_query import get_init_data

#even kijken of deze hier zo moet blijven staan als we gaan werken met airflow of dat het de uitput is van de vorige flow
df_policia = get_init_data()
df_policia.tosql(name='defaultdb',
                 con='postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require',
                 #databasetype://admin:wachtwoord@host:port/naam_database
                 if_exists='append' #of 'fail')