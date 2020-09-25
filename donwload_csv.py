import gzip
import io
import requests
import pandas
from datetime import datetime
from pymongo import MongoClient

url = "https://data.brasil.io/dataset/covid19/caso.csv.gz"

response = requests.get(url)
bytes_io = io.BytesIO(response.content)
with gzip.open(bytes_io, 'rt') as read_file:
    df = pandas.read_csv(read_file)


dt_sp = df.loc[df['state'] == 'SP']
df_filtered = dt_sp.dropna()
del df_filtered['place_type']
del df_filtered['death_rate']
del df_filtered['confirmed_per_100k_inhabitants']
del df_filtered['estimated_population_2019']
del df_filtered['city_ibge_code']
del df_filtered['order_for_place']
print(df_filtered)

# df_filtered.plot()
# plt.show()

try:
    client = MongoClient(
        "mongodb+srv://tcc:tcc_covid_19@cluster0.pmkga.mongodb.net/DB_COVID?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")
    print("Sucesso na conexão com o Mongo DB")
except:
    print("Erro na conexão com o Mongo DB")

db = client.DB_COVID
collection = db.covid_historicos

for index, row in df_filtered.iterrows():
    data = datetime.strptime(row['date'].replace('-', '/'), '%Y/%m/%d')
    data = data.strftime('%d/%m/%Y')
    post_data = {
        'state': row['state'],
        'cidade': row['city'],
        'data': data,
        'casos': row['confirmed'],
        'mortes': row['deaths']
    }
    result = collection.insert_one(post_data)
    print('Item inserido com sucesso na coleção: {0}'.format(result.inserted_id))
