from matplotlib import pyplot as plt
import pandas as pd
import numpy as np

##GET DATA FROM POSTGRESSQL DATABASE
#query SELECT * FROM ... (kan ook met pandas)
#df_policia =

df_policia = pd.read_sql_table('crime_api', con='postgres://avnadmin:qfycnefpdql4761s@pg-4b474b0-dennis-d0f3.aivencloud.com:25938/defaultdb?sslmode=require')
df_policia = df_policia.convert_dtypes()

##EXPLORATIEF ONDERZOEK

#SUMMARY STATISTICS
df_policia.info()
describe_data = df_policia.describe(include="all").transpose()

##PLAATSDELICT ONDERZOEK

#exploratief onderzoek naar plaatsdelict
count_plaatsen = df_policia["verdachte_plaatsdelict"].value_counts(dropna=True)
count_plaatsen_groter10 = count_plaatsen[count_plaatsen > 10]
#
#top 5 van plaats delicten
count_plaatsen[:5]
#genumereerde lijst met plaatsnaam en aantal
genumereerde_lijst = list(enumerate(zip(count_plaatsen.index, count_plaatsen)))
#vind op welke plaats Leiden staat
list(count_plaatsen.index).index("Leiden")

# #maak een barplot van het aantal
plt.bar(count_plaatsen_groter10.index, np.array(count_plaatsen_groter10), color='green')
plt.xlabel("Steden in Nederland")
plt.ylabel("Aantal verdachte gebeurtenissen")
plt.show()
#
# #maak eenn nieuwe kolom aan met enkel het jaartal van datum delcit
df_policia["verdachte_datumdelict_jaar"] = df_policia["verdachte_datumdelict"].dt.year
#bekijk het aantal verdachte incidenten per jaar
df_policia["verdachte_datumdelict_jaar"].value_counts()

##vind het aantal verdachte incidenten per plaat delict in 2020
df_policia.loc[df_policia["verdachte_datumdelict_jaar"] == 2020, "verdachte_plaatsdelict"].value_counts()

#vind het aantal verdachte incidenten per plaatsdelict, per jaar
cross_plaats_jaar = pd.crosstab(df_policia["verdachte_plaatsdelict"], df_policia["verdachte_datumdelict_jaar"])
cross_plaats_jaar = cross_plaats_jaar.sort_values(by=2020.0, ascending=False)
cross_plaats_jaar.loc[:, cross_plaats_jaar.columns > 2015]



