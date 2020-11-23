# TCC

Este projeto tem a finalidade de apresentar como utilizamos a linguagem Python no nosso TCC, utilizando os componentes nativos e aplicando uma inteligência artificial para prever os casos e mortes dos próximos 7 dias sobre a Covid-19, utilizando a base de dados do Brasil.io.

## Rotina

Ao compilar o código, o programa irá rodar uma rotina diária com os seguintes passos:

1 - Coletar a base de dados do [Brasil.io](https://brasil.io/home/) com os dados atuais via [arquivo csv](https://data.brasil.io/dataset/covid19/caso_full.csv.gz).

2 - Filtrar as informações apenas pelo estado de São Paulo.

3 - Criar uma conexão com o MongoDB.

4 - Inserir os dados coletados na coleção de histórico do MongoDB.

5 - Aplicar o ARIMA para prever os casos dos próximos 7 dias.

6 - Inserir as previsões na coleção de previsão do MongoDB.


## Colaboradores
[Felipe Costa Lourenço](https://github.com/Feelourenco)

[Matheus Alves Viana](https://github.com/matheusalvesviana)

[Patrick Garcia Soares](https://github.com/patriickgarcia11)
