import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "pipelines"))

from prefect import flow, task, get_run_logger
from extract import run_extraction
from transform import run_transform
from load import run_load


@task(name="Extração — Bronze (Raw)", retries=2, retry_delay_seconds=30)
def task_extract() -> dict:
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
    logger = get_run_logger()
    logger.info("Iniciando carga no DuckDB...")
    resultado = run_load()
    total = sum(v["registros"] for v in resultado.values())
    logger.info(f"Carga concluída: {total} registros em {len(resultado)} tabelas.")
    return resultado


@flow(name="ibge_pipeline", description="Pipeline completo IBGE: Bronze → Silver → Gold → DuckDB")
def ibge_pipeline():
    logger = get_run_logger()
    logger.info("🚀 Iniciando pipeline IBGE Lakehouse")

    resultado_extract   = task_extract()
    resultado_transform = task_transform(wait_for=[resultado_extract])
    resultado_load      = task_load(wait_for=[resultado_transform])

    logger.info("✅ Pipeline concluído com sucesso!")
    logger.info("Tabelas disponíveis no DuckDB: pib_per_capita, ranking_estados, ipca_historico")

    return {"extract": resultado_extract, "transform": resultado_transform, "load": resultado_load}


if __name__ == "__main__":
    ibge_pipeline()
