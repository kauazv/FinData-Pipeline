# FinData Pipeline

Pipeline de Engenharia e Análise de Dados Financeiros com dados reais (Yahoo Finance), modelagem analítica em DuckDB e baseline de ML.

## Objetivo

Simular um mini sistema de dados de fintech para:

- coleta de dados históricos de mercado
- processamento em camadas bronze/silver/gold
- validação de qualidade com gates
- carga em data warehouse analítico
- treino de modelos baseline de ML
- geração de artefatos e métricas reprodutíveis

## Arquitetura

Data Source (Yahoo Finance)  
↓  
Ingestion (`data/bronze`)  
↓  
Processing + Validation (`data/silver`)  
↓  
Feature Engineering (`data/gold`)  
↓  
Warehouse (DuckDB)  
↓  
ML Baseline (`ml/artifacts`)  
↓  
Analytics / Notebook / Dashboard

## Stack Atual

- Python 3.12
- Pandas, DuckDB, yfinance
- Scikit-learn (classificação + regressão baseline)
- Pytest (testes de pipeline)
- Docker (execução básica do pipeline)

## Estrutura do Projeto

- `ingestion/`: coleta de mercado e escrita em bronze
- `processing/`: normalização bronze→silver e criação de gold
- `validation/`: validação de qualidade da silver
- `warehouse/`: carga incremental no DuckDB + consulta analítica
- `pipelines/`: orquestração fim a fim (`run_pipeline.py`)
- `ml/`: treino baseline e artefatos de modelo/métricas
- `storage/runs/`: relatórios de execução do pipeline
- `notebooks/`: análises exploratórias e resultados
- `docker/`: execução do pipeline via container

## Configuração

1. Criar ambiente virtual e instalar dependências:

```bash
pip install -r requirements.txt
```

2. Criar arquivo `.env` a partir do exemplo:

```bash
cp .env.example .env
```

## Execução do Pipeline

Execução completa (inclui ingestão):

```bash
python pipelines/run_pipeline.py --with-ml
```

Execução local sem ingestão (útil para ambiente sem rede):

```bash
python pipelines/run_pipeline.py --skip-ingestion --with-ml
```

### Execução via Docker (base próxima de produção)

```bash
docker compose -f docker/docker-compose.yml up --build
```

Notas:

- o compose monta volumes para `data/`, `warehouse/`, `ml/artifacts/` e `storage/runs/`
- por padrão usa `--skip-ingestion --with-ml` para rodar mesmo sem acesso externo

## Testes

```bash
python -m pytest
```

Cobertura atual de testes:

- processamento bronze→silver (schema e deduplicação)
- validação de qualidade (empty/null/duplicidade)
- carga incremental no DuckDB e migração de schema

## Artefatos Gerados

- `storage/runs/run_<run_id>.json`: histórico de execução
- `ml/artifacts/metrics_baseline.json`: métricas dos modelos
- `ml/artifacts/best_model_classifier.joblib`: melhor classificador
- `ml/artifacts/best_model_regressor.joblib`: melhor regressor
- `ml/artifacts/supervised_dataset_latest.csv`: dataset supervisionado

## Baseline de ML

O baseline treina e compara:

- Classificação da direção do retorno D+1 (`target_up`)
- Regressão do retorno D+1 (`target_return_next_day`)

Com split temporal para reduzir risco de vazamento entre treino e teste.

## Notebook de Análise

Notebook inicial em:

- `notebooks/market_analysis.ipynb`

Ele lê `ml/artifacts/metrics_baseline.json` e compara modelos de classificação e regressão.

## Roadmap Curto

- expandir `features/` para feature store versionada
- conectar `dashboard/` ao DuckDB e métricas de ML
- adicionar orquestração agendada (Airflow/cron)

## Documentação Operacional

- Runbook: `docs/OPERATIONS_RUNBOOK.md`
- Arquitetura/decisões técnicas: `docs/ARCHITECTURE_DECISIONS.md`
