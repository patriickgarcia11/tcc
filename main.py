import requests
import time
from pymongo import MongoClient

# Pegando dados totais de SP (historico de casos em SP):
# https://brasil.io/api/dataset/covid19/caso/data/?page=1&state=SP
#
# Pegando dados atualizados de SP:
# https://brasil.io/covid19/cities/cases/?state=SP
# Pegando dados acumulados de SP

# Inicializando variáveis
url = f'https://brasil.io/covid19/cities/cases/?state=SP'
lista_dados = []


def connect_to_mongo():
    try:
        client = MongoClient(
            "mongodb+srv://tcc:tcc_covid_19@cluster0.pmkga.mongodb.net/DB_COVID?retryWrites=true&w=majority&ssl=true"
            "&ssl_cert_reqs=CERT_NONE")
        print("Sucesso na conexão com o Mongo DB")
        db = client.DB_COVID
        return db.DB_COVID
    except:
        print("Erro na conexão com o Mongo DB")
        return None


def get_covid_data(base_url):
    # Request na api, timeout de 15 segundos
    resposta = requests.get(base_url, timeout=15)

    if resposta.ok:
        return resposta.json()
    else:
        return None


collection = connect_to_mongo()
pagina = get_covid_data(url)


def insert_to_covid_collection(estado, cidade, data, confirmados, mortes):
    # Montando JSON a ser inserido
    post_data = {
        'estado': estado,
        'cidade': cidade,
        'data': data,
        'casos_confirmados': confirmados,
        'mortes': mortes
    }
    # Inserindo o JSON na coleção
    result = collection.insert_one(post_data)
    print('Item inserido com sucesso na coleção: {0}'.format(result.inserted_id))


while True:
    dados = pagina['results']

    for resultado in dados:
        estado = resultado["state"]
        cidade = resultado["city"]
        data = resultado["date"]
        confirmados = resultado["confirmed"]
        mortes = resultado["deaths"]

        print(f'Estado: {estado}')
        print(f'Cidade: {cidade}')
        print(f'Data de atualização: {data}')
        print(f'Confirmados: {confirmados}')
        print(f'Mortes: {mortes}\n')

        lista_dados.append(resultado)

        # Chamando método para inserir dados
        insert_to_covid_collection(estado, cidade, data, confirmados, mortes)

    if pagina['next'] is not None:
        print(f'Aguarde 10 segundos... {pagina["next"]}')
        time.sleep(10)
        resposta = requests.get(pagina['next'])
        if resposta.ok:
            pagina = resposta.json()
        else:
            print(f'Mais 10 segundos... {pagina["next"]}')
            time.sleep(10)
            resposta = requests.get(pagina['next'])
            if resposta.ok:
                pagina = resposta.json()
    else:
        print('A lista acabou')
        break

print(f'A lista acabou e os dados finais são {lista_dados}')
print(f'A lista acabou e tamanho da lista é de {len(lista_dados)} itens')

# Plotando dados com valores
# x = lista_data
# y = lista_mortes
# plot.bar(x, y)
# plot.show()
