import os
from google.cloud import bigquery

from extract_bcb import extrair

PROJECT = os.environ['GCP_PROJECT']
TABELA = f'{PROJECT}.raw.bcb_series'

SCHEMA = [
    bigquery.SchemaField('serie_id', 'INT64'),
    bigquery.SchemaField('data', 'DATE'),
    bigquery.SchemaField('valor', 'FLOAT64'),
    bigquery.SchemaField('data_carga', 'TIMESTAMP')
]

def carregar(df):
    client = bigquery.Client(project=PROJECT)
    config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition='WRITE_TRUNCATE')
    client.load_table_from_dataframe(df, TABELA, job_config=config).result()
    print(f'{client.get_table(TABELA).num_rows} linhas em {TABELA}')
    
    
if __name__ == '__main__':
    carregar(extrair())