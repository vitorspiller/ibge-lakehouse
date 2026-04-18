"""
load.py — Camada Gold (Refined) + DuckDB
=========================================
Responsável por:
- Ler os Parquets da camada trusted (Silver)
- Cruzar datasets para gerar tabelas analíticas (Gold)
- Salvar em data/refined/
- Registrar todas as tabelas no DuckDB (arquivo local ibge.duckdb)

Tabelas geradas:
- pib_per_capita      : PIB + população cruzados → PIB per capita por município
- ranking_estados     : PIB total e médio agregado por estado
- ipca_historico      : IPCA mensal com variação acumulada
"""

import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from datetime import datetime
import logging

# ─── Logs ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Caminhos ────────────────────────────────────────────────────────────────
TRUSTED_PATH = Path("data/trusted")
REFINED_PATH = Path("data/refined")
REFINED_PATH.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = Path("data/ibge.duckdb")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_latest_parquet(prefix: str, base: Path = TRUSTED_PATH) -> pd.DataFrame:
    """Lê o Parquet mais recente de um prefixo na pasta indicada."""
    files = sorted(base.glob(f"{prefix}_*.parquet"), reverse=True)
    if not files:
        raise FileNotFoundError(f"Nenhum Parquet encontrado para '{prefix}' em {base}")
    logger.info(f"  → Lendo: {files[0].name}")
    return pd.read_parquet(files[0])


def save_refined(df: pd.DataFrame, name: str) -> Path:
    """Salva a tabela Gold em Parquet."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = REFINED_PATH / f"{name}_{timestamp}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"  → Salvo em: {path} ({path.stat().st_size / 1024:.1f} KB)")
    return path


# ─── Transformações Gold ──────────────────────────────────────────────────────

def build_pib_per_capita(df_pib: pd.DataFrame, df_pop: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza PIB municipal com população estimada.
    Usa o período mais recente disponível em comum entre os dois datasets.
    Calcula PIB per capita em R$.
    """
    logger.info("Construindo: pib_per_capita")

    # Pega o ano mais recente de cada dataset
    ano_pib = int(df_pib["periodo"].max())
    ano_pop = int(df_pop["periodo"].max())
    logger.info(f"  → Ano PIB: {ano_pib} | Ano população: {ano_pop}")

    pib = df_pib[df_pib["periodo"] == ano_pib][["municipio_id", "municipio_nome", "estado_sigla", "pib_mil_reais"]]
    pop = df_pop[df_pop["periodo"] == ano_pop][["municipio_id", "populacao_estimada"]]

    df = pib.merge(pop, on="municipio_id", how="inner")

    # PIB em R$ mil → R$; per capita em R$
    df["pib_reais"]      = df["pib_mil_reais"] * 1_000
    df["pib_per_capita"] = (df["pib_reais"] / df["populacao_estimada"]).round(2)

    df["ano_pib"] = ano_pib
    df["ano_pop"] = ano_pop

    df = df.sort_values("pib_per_capita", ascending=False).reset_index(drop=True)
    df["ranking_nacional"] = df.index + 1

    logger.info(f"  → {len(df)} municípios com dado cruzado")
    return df[[
        "ranking_nacional", "municipio_id", "municipio_nome", "estado_sigla",
        "pib_mil_reais", "populacao_estimada", "pib_reais", "pib_per_capita",
        "ano_pib", "ano_pop"
    ]]


def build_ranking_estados(df_pib_per_capita: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega PIB e população por estado.
    Calcula PIB total, população total e PIB per capita médio por estado.
    """
    logger.info("Construindo: ranking_estados")

    df = (
        df_pib_per_capita
        .groupby("estado_sigla")
        .agg(
            pib_total_mil_reais  = ("pib_mil_reais",       "sum"),
            populacao_total      = ("populacao_estimada",  "sum"),
            municipios_count     = ("municipio_id",        "count"),
            pib_per_capita_medio = ("pib_per_capita",      "mean"),
        )
        .reset_index()
        .sort_values("pib_total_mil_reais", ascending=False)
        .reset_index(drop=True)
    )

    df["ranking"] = df.index + 1
    df["pib_per_capita_medio"] = df["pib_per_capita_medio"].round(2)

    logger.info(f"  → {len(df)} estados")
    return df[["ranking", "estado_sigla", "pib_total_mil_reais", "populacao_total",
               "municipios_count", "pib_per_capita_medio"]]


def build_ipca_historico(df_ipca: pd.DataFrame) -> pd.DataFrame:
    """
    Organiza o IPCA por período e calcula variação acumulada simples.
    """
    logger.info("Construindo: ipca_historico")

    df = df_ipca.sort_values("periodo").reset_index(drop=True)
    df["variacao_acumulada_pct"] = df["variacao_pct"].cumsum().round(4)

    logger.info(f"  → {len(df)} períodos")
    return df[["periodo", "variacao_pct", "variacao_acumulada_pct"]]


# ─── DuckDB ──────────────────────────────────────────────────────────────────

def load_to_duckdb(tabelas: dict[str, Path]) -> None:
    """
    Registra todas as tabelas Gold no DuckDB.
    Recria as tabelas a cada execução (DROP + CREATE).
    """
    logger.info(f"Carregando no DuckDB: {DUCKDB_PATH}")
    con = duckdb.connect(str(DUCKDB_PATH))

    for nome, parquet_path in tabelas.items():
        con.execute(f"DROP TABLE IF EXISTS {nome}")
        con.execute(f"""
            CREATE TABLE {nome} AS
            SELECT * FROM read_parquet('{parquet_path.as_posix()}')
        """)
        count = con.execute(f"SELECT COUNT(*) FROM {nome}").fetchone()[0]
        logger.info(f"  → Tabela '{nome}': {count} registros")

    # Mostra um exemplo de query para o usuário
    logger.info("DuckDB pronto. Exemplo de query:")
    logger.info("  SELECT * FROM pib_per_capita WHERE estado_sigla = 'RS' LIMIT 10")

    con.close()


def explorar_duckdb() -> None:
    """Abre o DuckDB e exibe um preview de cada tabela."""
    con = duckdb.connect(str(DUCKDB_PATH))
    tabelas = con.execute("SHOW TABLES").fetchdf()["name"].tolist()

    print("\n📊 Preview das tabelas no DuckDB:")
    print("=" * 60)
    for tabela in tabelas:
        print(f"\n🔹 {tabela}")
        df = con.execute(f"SELECT * FROM {tabela} LIMIT 5").fetchdf()
        print(df.to_string(index=False))
    con.close()


# ─── Pipeline principal ───────────────────────────────────────────────────────

def run_load() -> dict:
    logger.info("=" * 60)
    logger.info("Iniciando carga — Camada Gold (Refined) + DuckDB")
    logger.info("=" * 60)

    # Lê trusted
    df_pib = get_latest_parquet("pib_municipal")
    df_pop = get_latest_parquet("populacao_municipal")
    df_ipca = get_latest_parquet("ipca_mensal")

    # Constrói tabelas Gold
    df_pib_per_capita  = build_pib_per_capita(df_pib, df_pop)
    df_ranking_estados = build_ranking_estados(df_pib_per_capita)
    df_ipca_hist       = build_ipca_historico(df_ipca)

    # Salva Parquets Gold
    tabelas = {}
    tabelas["pib_per_capita"]  = save_refined(df_pib_per_capita,  "pib_per_capita")
    tabelas["ranking_estados"] = save_refined(df_ranking_estados, "ranking_estados")
    tabelas["ipca_historico"]  = save_refined(df_ipca_hist,       "ipca_historico")

    # Carrega no DuckDB
    load_to_duckdb(tabelas)

    resultados = {
        nome: {"registros": len(df), "path": str(path)}
        for (nome, path), df in zip(
            tabelas.items(),
            [df_pib_per_capita, df_ranking_estados, df_ipca_hist]
        )
    }

    logger.info("=" * 60)
    logger.info("Carga concluída com sucesso.")
    logger.info("=" * 60)

    return resultados


if __name__ == "__main__":
    resultado = run_load()

    print("\nResumo da carga:")
    for tabela, info in resultado.items():
        print(f"  ✅ {tabela}: {info['registros']} registros → {info['path']}")

    explorar_duckdb()
