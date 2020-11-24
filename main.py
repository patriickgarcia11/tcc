import pandas
import gzip
import datetime
import requests
import schedule
import time
from io import BytesIO
from dateutil import parser
from pymongo import MongoClient
from statsmodels.tsa.arima_model import ARIMA


# method to return the mongodb connection
def get_mongodb_database():
    try:
        client = MongoClient(
            "mongodb+srv://tcc:tcc_covid_19@cluster0.pmkga.mongodb.net/DB_COVID?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")
        print("Sucesso na conexão com o Mongo DB")
    except:
        print("Erro na conexão com o Mongo DB")

    db = client.DB_COVID
    return db


# method to insert the cumulative data
def insert_cumulative_data(mongo_database):
    url = "https://data.brasil.io/dataset/covid19/caso.csv.gz"
    collection = mongo_database.covid_historicos

    response = requests.get(url)
    bytes_io = BytesIO(response.content)
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

    for index, row in df_filtered.iterrows():
        data = datetime.datetime.strptime(row['date'].replace('-', '/'), '%Y/%m/%d')
        data = data.strftime('%d/%m/%Y')
        post_data = {
            'estado': row['state'],
            'cidade': row['city'],
            'data': data,
            'casos': row['confirmed'],
            'mortes': row['deaths']
        }
        result = collection.insert_one(post_data)
        print('Item inserido com sucesso na coleção covid_historicos: {0}'.format(result.inserted_id))


# method to get, filter and return the dataframe
def get_and_filter_data():
    url = "https://data.brasil.io/dataset/covid19/caso_full.csv.gz"

    response = requests.get(url)
    bytes_io = BytesIO(response.content)
    with gzip.open(bytes_io, 'rt') as read_file:
        df = pandas.read_csv(read_file)

    df = df.loc[df['state'] == 'SP']
    df = df.loc[df['city'] == 'São Paulo']
    df = df.dropna()
    del df['place_type']
    del df['epidemiological_week']
    del df['is_repeated']
    del df['last_available_confirmed_per_100k_inhabitants']
    del df['last_available_death_rate']
    del df['estimated_population']
    del df['is_last']
    del df['last_available_deaths']
    del df['last_available_confirmed']
    del df['last_available_date']
    del df['order_for_place']
    del df['estimated_population_2019']
    del df['city_ibge_code']
    return df


# method to insert the daily data to the collection
def insert_daily_data(df_filtered, mongo_database):
    collection = mongo_database.covid_diarios
    for index, row in df_filtered.iterrows():
        data = datetime.strptime(row['date'].replace('-', '/'), '%Y/%m/%d')
        data = data.strftime('%d/%m/%Y')
        data = parser.parse(data)

        post_data = {
            'estado': row['state'],
            'cidade': row['city'],
            'data': data,
            'casos': row['new_confirmed'],
            'mortes': row['new_deaths']
        }
        result = collection.insert_one(post_data)
        print('Item inserido com sucesso na coleção covid_diarios: {0}'.format(result.inserted_id))


# method to return the arima forecasts results
def apply_arima(index):
    model = ARIMA(index, order=(2, 1, 2))
    model_fit = model.fit(disp=0)
    forecast, _, _ = model_fit.forecast(7, alpha=0.05)
    model_fit.plot_predict(dynamic=False)
    print(model_fit.summary())
    print(model_fit)
    print(f'Previsão dos próximos 7 dias: {forecast}')
    return forecast


# method to insert the forecasts to the collection
def insert_forecasts(deaths_forecast, cases_forecast, df_filtered, mongo_database):
    collection = mongo_database.covid_previsoes

    last_date = df_filtered.date.max()
    last_date = datetime.datetime.strptime(last_date.replace('-', '/'), '%Y/%m/%d')
    last_date = last_date.strftime('%d/%m/%Y')
    last_date = parser.parse(last_date)
    print(f'Ultima data: {last_date}')

    days = 0
    for deaths, cases in zip(deaths_forecast, cases_forecast):
        days += 1
        prevision_date = last_date + datetime.timedelta(days=days)

        post_data = {
            'estado': 'SP',
            'cidade': 'São Paulo',
            'data': prevision_date.strftime('%d/%m/%Y'),
            'casos': int(cases),
            'mortes': int(deaths)
        }
        result = collection.insert_one(post_data)
        print('Item inserido com sucesso na coleção covid_previsoes: {0}'.format(result.inserted_id))


# method to run all the script routine
def run_routine():
    # get mongo db database by connection
    mongo_database = get_mongodb_database()

    # get filtered data frame
    df_filtered = get_and_filter_data()

    # insert historic data with the filtered data frame
    insert_daily_data(df_filtered, mongo_database)

    # insert cumulative data with the filtered data frame
    insert_cumulative_data(mongo_database)

    # get deaths forecast to the next 7 days
    deaths_forecast = apply_arima(df_filtered.new_deaths)

    # get cases forecast to the next 7 days
    cases_forecast = apply_arima(df_filtered.new_confirmed)

    # insert forecasts data with the filtered data frame
    insert_forecasts(deaths_forecast, cases_forecast, df_filtered, mongo_database)


schedule.every().day.at("23:00").do(run_routine)

while 1:
    schedule.run_pending()
    time.sleep(1)
