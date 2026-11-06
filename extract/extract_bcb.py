import time
from datetime import datetime, date, timezone

import pandas as pd
import requests

SERIES = {432:'Meta Selic',433:'IPCA mensal',1:'Dólar comercial'}
ANO_INICIAL = 2015
URL = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados'

def buscar_bloco(codigo, inicio, fim, tentativas=3):
    params = {
        'formato': 'json',
        'dataInicial': inicio.strftime('%d/%m/%Y'),
        'dataFinal': fim.strftime('%d/%m/%Y')
    }
    for n in range(1, tentativas + 1):
        try:
            resp = requests.get(URL.format(codigo=codigo), params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException:
            if n == tentativas:
                raise
            time.sleep(2 * n)
            
def extrair_serie(codigo):
    hoje = date.today()
    linhas = []
    for ano in range(ANO_INICIAL, hoje.year + 1):
        fim = min(date(ano, 12, 31), hoje)
        linhas.extend(buscar_bloco(codigo, date(ano, 1, 1), fim))
        time.sleep(0.5)
    
    if not linhas:
        raise ValueError(f'Série {codigo} voltou vazia')
    
    df = pd.DataFrame(linhas)
    df['serie_id'] = codigo
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y').dt.date
    df['valor'] = pd.to_numeric(df['valor'])
    
    return df[['serie_id', 'data', 'valor']]

def extrair():
    df = pd.concat([extrair_serie(c) for c in SERIES], ignore_index=True)
    df = df.drop_duplicates(subset=['serie_id', 'data'])
    df['data_carga'] = datetime.now(timezone.utc)
    
    return df

if __name__ == '__main__':
    print(extrair().groupby('serie_id')['data'].agg(['count', 'min', 'max']))