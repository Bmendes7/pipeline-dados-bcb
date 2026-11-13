import time
from datetime import datetime, date, timezone, timedelta

import pandas as pd
import requests

SERIES = {432:'Meta Selic',433:'IPCA mensal',1:'Dólar comercial'}
ANO_INICIAL = 2015
URL = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados'

class RespostaInvalida(RuntimeError):
    """Resposta do BCB sem dados(304, corpo vazio). Vale tentar de novo."""

def buscar_bloco(codigo, inicio, fim, tentativas=3):
    for n in range(1, tentativas + 1):
        ini = inicio - timedelta(days=5 * (n - 1))
        params = {
            'formato': 'json',
            'dataInicial': inicio.strftime('%d/%m/%Y'),
            'dataFinal': fim.strftime('%d/%m/%Y')
        }
        try:
            resp = requests.get(URL.format(codigo=codigo), params=params, timeout=30)
            resp.raise_for_status()
            if resp.status_code != 200 or not resp.text.strip():
                raise RespostaInvalida(
                    f'BCB respondeu {resp.status_code} em {resp.url}: {resp.text[:200]!r}'
                )
            return resp.json() 
        except (requests.RequestException, RespostaInvalida):
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
    df = df[df['data'] >= date(ANO_INICIAL, 1, 1)]
    df['valor'] = pd.to_numeric(df['valor'])
    
    return df[['serie_id', 'data', 'valor']]

def extrair():
    df = pd.concat([extrair_serie(c) for c in SERIES], ignore_index=True)
    df = df.drop_duplicates(subset=['serie_id', 'data'])
    df['data_carga'] = datetime.now(timezone.utc)
    
    return df

if __name__ == '__main__':
    print(extrair().groupby('serie_id')['data'].agg(['count', 'min', 'max']))