from init_query import get_init_data
import pandas as pd
import numpy as np
#even kijken of deze hier zo moet blijven staan als we gaan werken met airflow of dat het de uitput is van de vorige flow
df_policia = get_init_data()

df_policia = df_policia.dropna(how='all', axis=1)
df_policia['locaties']= df_policia['locaties'].astype('str')
df_policia = df_policia.convert_dtypes()

df_policia= df_policia.select_dtypes(exclude=['object'])
df_policia.index = np.array(range(1519, 0, -1))
df_policia=df_policia.sort_index()

#aanpassen kolomnamen van '-' naar '_'
df_policia.columns = df_policia.columns.str.replace('-','_')

#print(df_policia.dtypes)
from sqlalchemy import create_engine
engine = create_engine('postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require', echo=True)


#df_test = pd.DataFrame({"test1","test2","test3","test4",'2018-12-19 09:26:03.47',"test6","test7","test8",890890890,"test10","test11","test12","test13","test14","test15","test16","test17","test18",'2018-12-19 09:26:03.47',"test20","test21","test22","test23","test24","test25","test26","test27",'2018-12-19 09:26:03.47',"test29","test30","test31","test32","test33",'2018-12-19 09:26:03.47',"test35","test36","test37","test38","test39","test40","test41","test42","test43"})
df_policia.to_sql('crime_api', con=engine, if_exists='replace') #append

#engine.execute("SELECT * FROM crime_api").fetchall()

#df_policia.to_sql(name='crime_api',
 #                con='postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require',
  #               #databasetype://admin:wachtwoord@host:port/defaultdb
   #              if_exists='append')#of 'fail',)

