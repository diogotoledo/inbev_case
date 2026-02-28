# 🍺 BEES Data Engineering Case — InBev

Pipeline de dados seguindo a arquitetura Medallion **(Bronze → Silver → Gold)**,
orquestrado com **Apache Airflow** e containerizado com **Docker Compose**.

## Arquitetura

```
Open Brewery DB API
        │
   [Bronze Layer]  ── JSON raw salvo com timestamp
        │
   [Silver Layer]  ── Parquet particionado por país / estado
        │
    [Gold Layer]   ── Parquet agregado por tipo de cervejaria + localidade
        │
 [Data Quality]   ── Validações automáticas via Airflow task
```

## Estrutura do Projeto

```
inbev-case/
├── dags/
│   └── brewery_pipeline.py       # DAG principal
├── src/
│   ├── bronze/ingest_api.py      # Ingestão da API
│   ├── silver/transform.py       # Transformação → Parquet particionado
│   └── gold/aggregate.py         # Agregação por tipo e localização
├── tests/
│   ├── test_ingest.py
│   └── test_transform.py
├── notebooks/
│   ├── 01_bronze_exploration.ipynb   # Exploração da camada raw
│   ├── 02_silver_exploration.ipynb   # Exploração da camada silver
│   └── 03_gold_exploration.ipynb     # Exploração da camada gold
├── data/                         # Data lake local (bronze/silver/gold)
├── docker-compose.yml
├── requirements.txt              # Dependências do pipeline (Docker)
├── requirements-dev.txt          # Dependências de desenvolvimento local
└── .env
```

## Como executar

### Pré-requisitos
- Docker Desktop instalado e rodando

### 1. Clone o repositório
```bash
git clone [<repo-url>](https://github.com/diogotoledo/inbev_case)
cd inbev-case
```

### 2. Suba os containers
```bash
docker-compose up --build
```
Aguarde ~2 minutos na primeira execução.

### 3. Acesse o Airflow
- URL: http://localhost:8080
- Usuário: `admin` | Senha: `admin`

### 4. Execute o pipeline
- Ative a DAG **brewery_pipeline**
- Clique em **Trigger DAG ▶**

### 5. Rode os testes
```bash
docker-compose exec airflow-scheduler pytest tests/ -v
```

## Análise Exploratória (opcional)

Esta análise foi incluída por questões de visualização somente, me considero uma pessoa muito visual,
então gosto de visualizar os dados em tabelas para estabelecer algumas relações inerentes ao pipeline.

Os notebooks de exploração rodam **localmente**, fora do Docker.
Instale as dependências de desenvolvimento e suba o Jupyter:

```bash
# Instalar dependências de desenvolvimento (inclui Jupyter)
pip install -r requirements-dev.txt

# Subir o Jupyter Notebook
jupyter notebook
```

Acesse a pasta `notebooks/` e abra os notebooks na ordem:

|        Notebook         |   Camada   |                     Conteúdo                               |
|-------------------------|------------|------------------------------------------------------------|
| `01_bronze_exploration` | 🟫 Bronze | Dados brutos da API, nulos, distribuição por país/tipo      |
| `02_silver_exploration` | 🥈 Silver | Partições Parquet, tipos de dados, coordenadas geográficas  |
| `03_gold_exploration`   | 🥇 Gold   | Agregações por tipo/país/estado, pivot tables, data quality |

> **Nota:** os notebooks usam caminhos relativos (`../data/`), portanto devem
> ser executados a partir da pasta `notebooks/`.

## Design Choices

|       Decisão       |               Escolha                   |                            Motivo                                |
|---------------------|-----------------------------------------|------------------------------------------------------------------|
| Orquestração        | Apache Airflow                          | Padrão enterprise, retry e scheduling nativos                    |
| Formato silver/gold | Parquet                                 | Columnar, eficiente para leitura analítica                       |
| Particionamento     | country + state                         | Otimiza queries por localização                                  |
| Containerização     | Docker Compose                          | Ambiente 100% reproduzível                                       |
| Monitoramento       | Data Quality Task no Airflow            | Detecta dados inválidos ou ausentes                              |
| Dependências        | requirements.txt + requirements-dev.txt | Separa dependências de runtime das de desenvolvimento            |

## Monitoramento e Alertas

- **Retries automáticos**: 3 tentativas com intervalo de 5 minutos por task
- **Data Quality Task**: valida volume, nulos e integridade do gold layer após cada run
- **Logs centralizados**: disponíveis na UI do Airflow por task e execução
- **Extensão sugerida**: Airflow Connections com Slack/e-mail para alertas em falha de produção

## Limitações conhecidas da fonte de dados

A **Open Brewery DB** é um dataset open-source mantido por contribuições voluntárias
da comunidade via Pull Requests no GitHub. Por esse motivo, o dataset é composto
principalmente por países de língua inglesa, sendo os seguintes os países atualmente disponíveis:

> Australia, Austria, Canada, England, France, Germany, Ireland, Isle of Man, Italy,
> Japan, Poland, Portugal, Scotland, Singapore, South Africa, South Korea, Sweden,
> Ukraine e United States.

**Países como Brasil não estão presentes na fonte de dados.** Isso não é uma limitação
do pipeline, mas sim da origem dos dados. O pipeline processa corretamente todos os
registros disponibilizados pela API.

### Extensão sugerida para outras fontes

Para cobrir mercados não presentes na Open Brewery DB (como o Brasil), o pipeline
poderia ser estendido para ingerir dados de fontes complementares, como:

- **Untappd API** — base de dados global de cervejas e cervejarias
- **RateBeer API** — catálogo global com cobertura de países da América Latina
- **Scraping de associações locais** — ex: CervBrasil (Associação Brasileira de Cerveja Artesanal)

A arquitetura Medallion adotada facilita essa extensão: bastaria adicionar novos
operadores na camada Bronze para cada fonte adicional, mantendo as camadas Silver
e Gold agnósticas à origem dos dados.
