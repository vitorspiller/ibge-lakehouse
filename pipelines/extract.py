import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

DATASETS = {
    "pib_municipal": {
        "tabela": "5938",
        "variaveis": "37",
        "classificacao": "N6[all]",
        "periodos": "2020|2021",
        "descricao": "PIB Municipal",
    },
    "populacao_municipal": {
        "tabela": "6579",
        "variaveis": "9324",
        "classificacao": "N6[all]",
        "periodos": "2021|2022",
        "descricao": "População Estimada por Município",
    },
    "ipca_mensal": {
        "tabela": "1737",
        "variaveis": "63",
        "classificacao": "N1[all]",
        "periodos": "202001|202101|202201|202301",
        "descricao": "IPCA Mensal",
    },
}


def build_url(config: dict) -> str:
    tabela = config["tabela"]
    variaveis = config["variaveis"]
    periodos = config["periodos"]
    classificacao = config["classificacao"]
    return f"{BASE_URL}/{tabela}/periodos/{periodos}/variaveis/{variaveis}?localidades={classificacao}&formato=json"


def fetch_data(name: str, config: dict) -> pd.DataFrame:
    url = build_url(config)
    logger.info(f"Buscando: {config['descricao']} | URL: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.Timeout:
        logger.error(f"Timeout ao buscar {name}.")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP ao buscar {name}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar {name}: {e}")
        return pd.DataFrame()

    rows = []
    for resultado in data:
        variavel_id = resultado.get("id")
        variavel_nome = resultado.get("variavel")
        for serie in resultado.get("resultados", []):
            for localidade in serie.get("series", []):
                loc_id = localidade["localidade"]["id"]
                loc_nome = localidade["localidade"]["nome"]
                for periodo, valor in localidade["serie"].items():
                    rows.append({
                        "variavel_id": variavel_id,
                        "variavel_nome": variavel_nome,
                        "localidade_id": loc_id,
                        "localidade_nome": loc_nome,
                        "periodo": periodo,
                        "valor_raw": valor,
                        "dataset": name,
                        "extraido_em": datetime.utcnow().isoformat(),
                    })

    df = pd.DataFrame(rows)
    logger.info(f"  → {len(df)} registros extraídos de {config['descricao']}")
    return df


def save_parquet(df: pd.DataFrame, name: str) -> Path:
    if df.empty:
        logger.warning(f"DataFrame vazio para {name}. Nada será salvo.")
        return None

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"{name}_{timestamp}.parquet"
    df.to_parquet(file_path, index=False, engine="pyarrow")
    logger.info(f"  → Salvo em: {file_path} ({file_path.stat().st_size / 1024:.1f} KB)")
    return file_path


def run_extraction() -> dict:
    logger.info("=" * 60)
    logger.info("Iniciando extração — Camada Bronze (Raw)")
    logger.info("=" * 60)

    resultados = {}
    for name, config in DATASETS.items():
        df = fetch_data(name, config)
        path = save_parquet(df, name)
        resultados[name] = {"path": str(path) if path else None, "registros": len(df), "sucesso": path is not None}

    logger.info("=" * 60)
    sucessos = sum(1 for r in resultados.values() if r["sucesso"])
    logger.info(f"Extração concluída: {sucessos}/{len(DATASETS)} datasets salvos.")
    logger.info("=" * 60)
    return resultados


if __name__ == "__main__":
    resultado = run_extraction()
    print("\nResumo da extração:")
    for dataset, info in resultado.items():
        status = "✅" if info["sucesso"] else "❌"
        print(f"  {status} {dataset}: {info['registros']} registros → {info['path']}")
