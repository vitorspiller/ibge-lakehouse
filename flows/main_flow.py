"""
main_flow.py — Orquestração com Prefect
========================================
Responsável por:
- Encadear extract → transform → load em um único flow
- Registrar logs e status de cada etapa
- Permitir rodar o pipeline completo com um único comando

Como rodar:
    python flows/main_flow.py

Como agendar (opcional, roda todo dia às 6h):
    prefect deployment build flows/main_flow.py:ibge_pipeline -n "diario" --cron "0 6 * * *"
    prefect deployment apply ibge_pipeline-deployment.yaml
    prefect agent start -q default
"""

import sys
from pathlib import Path

# Garante que os módulos em /pipelines sejam encontrados
sys.path.append(str(Path(__file__).parent.parent / "pipelines"))

from prefect import flow, task, get_run_logger

from extract import run_extraction
from transform import run_transform
from load import run_load


# ─── Tasks ───────────────────────────────────────────────────────────────────

@task(name="Extração — Bronze (Raw)", retries=2, retry_delay_seconds=30)
def task_extract() -> dict:
    """
    Busca dados da API SIDRA do IBGE e salva em Parquet bruto.
    Tenta até 2 vezes em caso de falha de rede.
    """
    logger = get_run_logger()
    logger.info("Iniciando extração dos dados do IBGE...")

    resultado = run_extraction()

    falhas = [k for k, v in resultado.items() if not v["sucesso"]]
    if falhas:
        raise ValueError(f"Extração falhou para: {falhas}")

    total = sum(v["registros"] for v in resultado.values())
    logger.info(f"Extração concluída: {total} registros no total.")
    return resultado


@task(name="Transformação — Silver (Trusted)")
def task_transform() -> dict:
    """
    Limpa e tipifica os dados brutos, salvando na camada trusted.
    """
    logger = get_run_logger()
    logger.info("Iniciando transformação dos dados...")

    resultado = run_transform()

    falhas = [k for k, v in resultado.items() if not v["sucesso"]]
    if falhas:
        raise ValueError(f"Transformação falhou para: {falhas}")

    total = sum(v["registros"] for v in resultado.values())
    logger.info(f"Transformação concluída: {total} registros no total.")
    return resultado


@task(name="Carga — Gold (Refined) + DuckDB")
def task_load() -> dict:
    """
    Gera tabelas analíticas e carrega no DuckDB.
    """
    logger = get_run_logger()
    logger.info("Iniciando carga no DuckDB...")

    resultado = run_load()

    total = sum(v["registros"] for v in resultado.values())
    logger.info(f"Carga concluída: {total} registros em {len(resultado)} tabelas.")
    return resultado


# ─── Flow principal ───────────────────────────────────────────────────────────

@flow(
    name="ibge_pipeline",
    description="Pipeline completo IBGE: Bronze → Silver → Gold → DuckDB",
)
def ibge_pipeline():
    """
    Flow principal que encadeia as 3 etapas do lakehouse.

    Bronze  → extração bruta da API do IBGE
    Silver  → limpeza e tipagem
    Gold    → tabelas analíticas cruzadas + DuckDB
    """
    logger = get_run_logger()
    logger.info("🚀 Iniciando pipeline IBGE Lakehouse")

    # Etapa 1: Extração
    resultado_extract = task_extract()

    # Etapa 2: Transformação (só roda se extração foi bem)
    resultado_transform = task_transform(wait_for=[resultado_extract])

    # Etapa 3: Carga (só roda se transformação foi bem)
    resultado_load = task_load(wait_for=[resultado_transform])

    logger.info("✅ Pipeline concluído com sucesso!")
    logger.info("Tabelas disponíveis no DuckDB: pib_per_capita, ranking_estados, ipca_historico")

    return {
        "extract":   resultado_extract,
        "transform": resultado_transform,
        "load":      resultado_load,
    }


# ─── Entrada ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ibge_pipeline()
