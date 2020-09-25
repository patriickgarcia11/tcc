from pymongo import MongoClient

try:
    client = MongoClient("mongodb+srv://tcc:tcc_covid_19@cluster0.pmkga.mongodb.net/DB_COVID?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")
    print("Sucesso na conexão com o Mongo DB")
except:
    print("Erro na conexão com o Mongo DB")

db = client.DB_COVID
collection = db.covid_historicos

post_data = {
    'state': 'SP',
    'cidade': 'Campinas',
    'casos': 50_000,
    'mortes': 10_000
}
result = collection.insert_one(post_data)
print('Item inserido com sucesso na coleção: {0}'.format(result.inserted_id))

# Zerar BD
result = collection.delete_many({})

