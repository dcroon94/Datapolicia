print("hello, world!")
import requests

##laad politie API in

# https://www.politie.nl/binaries/content/assets/politie/algemeen/algemeen/politieapi.pdf
# request.get("https://api.politie.nl/v4/gezocht?language=nl&lat=53.1511173&lon=6.756634599999984&radius=5.0&maxnumberofitems=10&offset=0")

api_url = "https://api.politie.nl/v4/gezocht"
params = {"language": "nl",
          "maxnumberofitems": "25"}

response = requests.get(url=api_url,
             params=params)

if response.status_code == 500:
    raise TypeError("Fout in de request, controleer de URL")
elif response.status_code == 400:
    raise TypeError("Fout in de request, controleer de parameters")
else:
    data_gezocht = response.json()

print(data_gezocht)


