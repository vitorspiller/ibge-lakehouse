import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

RAW_PATH     = Path("data/raw")
TRUSTED_PATH = Path("data/trusted")
TRUSTED_PATH.mkdir(parents=True, exist_ok=True)

ESTADO_MAP = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
    "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
    "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
    "52": "GO", "53": "DF",
}


def get_latest_parquet(prefix: str) -> Path | None:
    files = sorted(RAW_PATH.glob(f"{prefix}_*.parquet"), reverse=True)
    if not files:
        logger.warning(f"Nenhum arquivo encontrado para '{prefix}' em {RAW_PATH}")
        return None
    logger.info(f"  → Lendo: {files[0].name}")
    return files[0]


def parse_valor(series: pd.Series) -> pd.Series:
    return (
        series.astype(str).str.strip()
        .replace({"-": np.nan, "...": np.nan, "": np.nan, "None": np.nan})
        .pipe(pd.to_numeric, errors="coerce")
    )


def extrair_codigo_estado(localidade_id: pd.Series) -> pd.Series:
    return localidade_id.astype(str).str[:2].map(ESTADO_MAP)


def normalizar_nome(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.title()


def transform_pib_municipal(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando: PIB Municipal")
    df = df.copy()
    df["valor"]          = parse_valor(df["valor_raw"])
    df["periodo"]        = pd.to_numeric(df["periodo"], errors="coerce").astype("Int64")
    df["municipio_nome"] = normalizar_nome(df["localidade_nome"])
    df["municipio_id"]   = df["localidade_id"].astype(str).str.strip()
    df["estado_sigla"]   = extrair_codigo_estado(df["localidade_id"])

    antes = len(df)
    df = df.dropna(subset=["valor"])
    logger.info(f"  → Removidos {antes - len(df)} registros sem valor ({len(df)} restantes)")

    return df[["municipio_id", "municipio_nome", "estado_sigla", "periodo", "valor", "variavel_nome", "extraido_em"]].rename(columns={"valor": "pib_mil_reais"})


def transform_populacao_municipal(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando: População Municipal")
    df = df.copy()
    df["valor"]          = parse_valor(df["valor_raw"])
    df["periodo"]        = pd.to_numeric(df["periodo"], errors="coerce").astype("Int64")
    df["municipio_nome"] = normalizar_nome(df["localidade_nome"])
    df["municipio_id"]   = df["localidade_id"].astype(str).str.strip()
    df["estado_sigla"]   = extrair_codigo_estado(df["localidade_id"])

    antes = len(df)
    df = df.dropna(subset=["valor"])
    logger.info(f"  → Removidos {antes - len(df)} registros sem valor ({len(df)} restantes)")

    return df[["municipio_id", "municipio_nome", "estado_sigla", "periodo", "valor", "extraido_em"]].rename(columns={"valor": "populacao_estimada"})


def transform_ipca_mensal(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transformando: IPCA Mensal")
    df = df.copy()
    df["valor"]   = parse_valor(df["valor_raw"])
    df["periodo"] = pd.to_numeric(df["periodo"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["valor"])
    return df[["periodo", "valor", "variavel_nome", "extraido_em"]].rename(columns={"valor": "variacao_pct"})


TRANSFORMS = {
    "pib_municipal":       transform_pib_municipal,
    "populacao_municipal": transform_populacao_municipal,
    "ipca_mensal":         transform_ipca_mensal,
}


def save_trusted(df: pd.DataFrame, name: str) -> Path:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = TRUSTED_PATH / f"{name}_{timestamp}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"  → Salvo em: {path} ({path.stat().st_size / 1024:.1f} KB)")
    return path


def run_transform() -> dict:
    logger.info("=" * 60)
    logger.info("Iniciando transformação — Camada Silver (Trusted)")
    logger.info("=" * 60)

    resultados = {}
    for name, transform_fn in TRANSFORMS.items():
        raw_path = get_latest_parquet(name)
        if raw_path is None:
            resultados[name] = {"sucesso": False, "path": None, "registros": 0}
            continue

        df_raw     = pd.read_parquet(raw_path)
        df_trusted = transform_fn(df_raw)
        path       = save_trusted(df_trusted, name)
        resultados[name] = {"sucesso": True, "path": str(path), "registros": len(df_trusted)}

    logger.info("=" * 60)
    sucessos = sum(1 for r in resultados.values() if r["sucesso"])
    logger.info(f"Transformação concluída: {sucessos}/{len(TRANSFORMS)} datasets salvos.")
    logger.info("=" * 60)
    return resultados


if __name__ == "__main__":
    resultado = run_transform()
    print("\nResumo da transformação:")
    for dataset, info in resultado.items():
        status = "✅" if info["sucesso"] else "❌"
        print(f"  {status} {dataset}: {info['registros']} registros → {info['path']}")
