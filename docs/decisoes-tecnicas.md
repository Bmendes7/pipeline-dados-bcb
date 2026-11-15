# Decisões técnicas

Registro curto das decisões de projeto e dos trade-offs aceitos. Cada item segue o formato **contexto, decisão, consequência**. Atualizado em 15/11/2026.

## Parte 1: ELT com BigQuery e dbt

### 1. ELT em vez de ETL
- **Contexto:** o dado do BCB chega cru (datas e valores em texto) e o BigQuery faz a transformação com facilidade.
- **Decisão:** o Python só extrai e carrega o dado bruto em `raw.bcb_series`; toda a transformação é SQL versionado no dbt.
- **Consequência:** a regra de negócio fica testável e documentada em um lugar só. O custo é manter duas camadas (`raw` e `analytics`).

### 2. Carga completa com `WRITE_TRUNCATE` (idempotência)
- **Contexto:** o volume é pequeno (cerca de 7,4 mil linhas, 3 séries) e o pipeline precisa poder rodar de novo sem duplicar dados (retry, disparo manual, execução dobrada).
- **Decisão:** cada carga substitui a tabela `raw.bcb_series` inteira por um *load job*, que só troca a tabela quando termina com sucesso.
- **Consequência:** repetir a carga é seguro e uma falha no meio não deixa a tabela pela metade (comprovado em 13/11/2026, veja o README). Em volumes maiores seria necessária carga incremental.

### 3. Modelo dimensional com chave substituta
- **Decisão:** `fct_indicadores` tem granularidade explícita (uma linha por série e data), com `indicador_id` (hash MD5 de `serie_id` e data) para poder testar `unique` e `not_null` na granularidade.
- **Consequência:** a unicidade da granularidade é verificada a cada execução do `dbt test`.

### 4. `dim_serie` a partir de seed (CSV versionado)
- **Contexto:** o cadastro das séries é pequeno e muda raramente.
- **Decisão:** `dbt/seeds/series_bcb.csv` em vez de uma tabela mantida à mão.
- **Consequência:** o cadastro é versionado e testado. Em contrapartida, o `dim_serie` depende de `dbt seed`; por isso a task `dbt_run` do Airflow roda `dbt seed && dbt run`, e a DAG funciona também em um dataset novo.

### 5. `dim_data` gerada por SQL
- **Decisão:** `generate_date_array('2010-01-01', '2030-12-31')`, sem depender dos dados carregados.
- **Consequência:** o calendário nunca tem buracos. O intervalo é fixo e precisa ser estendido antes de 2030.

### 6. Staging e marts como *views*
- **Decisão:** `+materialized: view` em `dbt_project.yml` para as duas camadas.
- **Consequência:** nenhum armazenamento a gerenciar (o sandbox expira tabelas em 60 dias) e o dado sempre reflete a `raw`. Cada consulta recalcula; com mais volume, os marts virariam tabelas.

### 7. Testes de dados em todas as camadas, em dois formatos
- **Contexto:** no início só os marts e o seed tinham testes (14 no total); a staging e o `mart_indicadores_mensais` ficaram sem cobertura.
- **Decisão:** testes genéricos nos `.yml` (`not_null`, `unique`, `accepted_values`, `relationships`) para as regras simples, e **testes singulares** (SQL em `dbt/tests/`, que devolve as linhas com problema) para regras de negócio: chave única na staging e no mart, e valor positivo para dólar e Selic. O IPCA fica de fora da regra de positividade porque pode ser negativo (deflação). Os singulares dispensam o pacote `dbt_utils`.
- **Consequência:** 26 testes de dados, todos passando. A unicidade é verificada na staging, onde uma duplicata da `raw` apareceria primeiro. Não se colocou `not_null` em `variacao_mes_pct`, que é nula no primeiro mês de cada série (efeito do `lag()`).

## Parte 2: Orquestração com Airflow

### 8. Airflow com o Docker Compose oficial
- **Decisão:** o `docker-compose.yaml` oficial (CeleryExecutor), ajustado: imagem customizada, volumes e variáveis de ambiente.
- **Consequência:** ambiente fiel a um Airflow real e reprodutível com poucos comandos. O Compose é para aprendizado, não produção, e o agendamento só dispara com o Docker ligado: em 12/11 a execução das 07:00 só começou às 09:57, e as de 14 e 15/11 só começaram às 14:16, quando o Docker foi religado.

### 9. Código do projeto montado como volume
- **Decisão:** `../extract` e `../dbt` são montados em `/opt/airflow/project/`.
- **Consequência:** o Airflow roda o mesmo código que roda no terminal, sem cópia. Uma edição vale na hora, sem *rebuild* da imagem.

### 10. Credencial do BigQuery montada como arquivo, fora do Git
- **Decisão:** `airflow/secrets/gcp-sa.json` (somente leitura no contêiner), ignorado pelo Git, apontado por `GOOGLE_APPLICATION_CREDENTIALS`. O profile do dbt do contêiner (`airflow/dbt_profiles/profiles.yml`, sem segredo) usa `method: oauth`.
- **Consequência:** neste projeto o arquivo é a credencial do `gcloud auth application-default login`, e não uma service account. Funciona no estudo local, mas é uma credencial pessoal com *refresh token* que pode ser revogada (erro `invalid_grant`). Em uso real: service account com BigQuery Data Editor e Job User.

### 11. dbt em virtualenv separado, com versões fixadas
- **Contexto:** a instalação sem versão resolveu para o `dbt-core 1.13.0rc1`, uma versão candidata. Ela trazia como dependência o `dbt-state`, um plugin do serviço dbt platform que abre um login no navegador ao rodar `dbt test` localmente. Ao voltar para o core estável, o pacote ficou órfão no ambiente local e o login continuou aparecendo, até ser desinstalado (`pip uninstall dbt-state`).
- **Decisão:** `dbt-core==1.12.7` e `dbt-bigquery==1.12.2`, no `Dockerfile` (virtualenv `/home/airflow/dbt_venv`) e no `requirements.txt` da raiz.
- **Consequência:** sem conflito com as dependências do Airflow, ambiente local e contêiner na mesma versão e builds reproduzíveis. Foi confirmado que o contêiner reconstruído não tem o `dbt-state`.

### 12. Três tasks, cada uma com um papel
- **Decisão:** `extrair_e_carregar` (`@task`, TaskFlow), `dbt_run` e `dbt_test` (`BashOperator`), encadeadas com `>>`.
- **Consequência:** no Graph view se vê se quebrou na carga, na transformação ou no teste. O `import load_bq` fica dentro da task para que a leitura periódica do arquivo da DAG não carregue pandas e BigQuery.

### 13. Agendamento diário, sem recuperar datas passadas
- **Decisão:** `schedule="0 7 * * *"`, `start_date` em `America/Sao_Paulo`, `catchup=False`.
- **Consequência:** como a carga é completa e não usa a data lógica, não existe o que "recuperar" (por isso Jinja `{{ ds }}` e XCom não são usados).

### 14. Retentativas e timeouts por task
- **Decisão:** `retries=3` e `retry_delay` lido de `RETRY_DELAY_SEC` (padrão 300 s), via `default_args`. A task `dbt_test` sobrescreve com `retries=0`. `execution_timeout`: 10 min em `extrair_e_carregar`, 5 min em `dbt_run` e 15 min em `dbt_test`.
- **Por que `retries=0` no teste:** erro de rede ou de API pode passar sozinho, mas um teste de dado que falha falharia de novo. Com 3 retentativas de 5 minutos, o alerta de um problema de dado chegaria 15 minutos atrasado.
- **Timeouts:** servem para pegar tasks travadas, e não para ser justos. Em 15/11 as tasks levaram cerca de 60 s, 40 s e 22 s. O pior caso da extração (36 requisições com espera de 2 s e 4 s nos retries internos) passa de 3 minutos, por isso os 10 minutos.
- **Consequência:** o atraso entre tentativas pode ser reduzido a 10 s para testes, sem mexer no código. O retry do Airflow se soma ao retry curto do `extract_bcb.py` (3 tentativas), então os dois se multiplicam.

### 15. Alerta de falha definitiva no Slack
- **Contexto:** o plano pedia e-mail ou Teams.
- **Decisão:** `on_failure_callback` que envia um webhook de entrada do Slack. O Slack é o padrão em times de dados, não depende do ambiente corporativo e o webhook é só uma URL. O e-mail exigiria SMTP no Airflow.
- **Consequência:** o callback roda uma única vez, quando os retries se esgotam (as tasks `upstream_failed` não geram alerta). A URL fica em `ALERT_WEBHOOK_URL` (variável genérica, o serviço pode ser trocado sem mexer no compose). Sem a variável, o texto só é impresso no log. A função não usa `raise_for_status()`, porque a mensagem de erro do `requests` incluiria a URL secreta no log.

### 16. Tratamento do `304 Not Modified` da API do BCB
- **Contexto:** em 13/11/2026 a API devolveu 304 com corpo vazio para a série 433 (`dataInicial=01/01/2026`), de forma reproduzível com `curl`. A resposta traz `cache-control: public, max-age=900`; consultas com início diferente respondem 200.
- **Decisão:** resposta vazia ou não 200 vira `RespostaInvalida`, que entra no retry interno. A cada nova tentativa, o `dataInicial` recua 5 dias (muda a URL e escapa do cache). A sobreposição some no `drop_duplicates`, e um filtro `>= 2015-01-01` impede dados de antes do ano inicial. Não se usou `dataFinal=31/12`, porque a Selic devolveria datas futuras.
- **Consequência:** o 304 deixa de derrubar a execução. O comportamento foi validado por uma simulação (304, 304 e depois 200), em que as tentativas enviaram `01/01/2026`, `27/12/2025` e `22/12/2025`. Depois da correção o 304 não voltou a ocorrer, então ela ainda não foi exercitada contra a API real. A causa está no lado do BCB; a mitigação não a elimina.

### 17. Segredos fora do Git
- **Decisão:** `.env`, `airflow/secrets/` e `airflow/config/airflow.cfg` ficam no `.gitignore`.
- **Consequência:** o `airflow.cfg` gerado pelo Compose contém `fernet_key`, `secret_key` e `jwt_secret`. Ele chegou a entrar em um commit local, que foi corrigido (*amend*) antes de qualquer `push`, em um repositório público.

## Decisões deixadas de fora de propósito
- **XCom e Jinja (`{{ ds }}`):** as tasks não trocam dados e a carga não depende da data.
- **Carga incremental e backfill:** o volume não justifica e o sandbox restringe DML.
- **Retry policies (Airflow 3.4) e `AirflowFailException`:** servem para não repetir erros sem solução (credencial inválida), e ficam para uma próxima iteração.
- **Teste de frescor (`dbt source freshness`):** não implementado.
