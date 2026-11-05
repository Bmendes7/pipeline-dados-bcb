# Pipeline ELT de indicadores do Banco Central (BigQuery + dbt)

> Projeto em construção (D1 da trilha de dados).

## Problema
Indicadores públicos do Banco Central (Selic, IPCA, dólar) ficam em uma API,
sem histórico organizado nem testes de qualidade. Este projeto extrai, carrega
e transforma esses dados em tabelas prontas para análise.

## Arquitetura
API do BCB -> extract (Python) -> BigQuery `raw` -> dbt staging -> dbt marts -> dashboard

## Stack
Python, BigQuery (sandbox), dbt Core, Power BI / Looker Studio, GitHub.

## Estrutura do repositório
- `extract/`: scripts Python de extração e carga no BigQuery
- `dbt/`: projeto dbt (staging e marts)
- `docs/img/`: prints da linhagem e do dashboard

## Modelagem
_A preencher (granularidade da fato, dimensões)._

## Qualidade de dados
_A preencher (testes do dbt)._

## Como rodar
_A preencher conforme os passos forem concluídos._

## Resultado
_Prints do dashboard e da linhagem (`docs/img/`)._

## Limitações e próximos passos
- BigQuery sandbox expira tabelas em 60 dias e não aceita DML.
- Próximos: orquestração com Airflow (D2) e API sobre o warehouse (D3).

## Autor
Brendon Mendes - [LinkedIn](https://www.linkedin.com/in/brendonmendes)