IBGE Lakehouse
Pipeline de dados end-to-end com arquitetura medallion usando dados reais da API SIDRA/IBGE.

```
Como rodar
1. Clone o repositório
```bash
git clone https://github.com/vitorspiller/ibge-lakehouse.git
cd ibge-lakehouse
```
2. Instale as dependências
```bash
pip install requests pandas pyarrow duckdb prefect streamlit plotly
```
3. Execute o pipeline completo
```bash
python flows/main_flow.py
```
4. Abra o dashboard
```bash
python -m streamlit run app/dashboard.py
```
Acesse em `http://localhost:8501`

Estrutura do projeto:
```
ibge-lakehouse/
├── pipelines/
│   ├── extract.py        # Camada Bronze — extração da API IBGE
│   ├── transform.py      # Camada Silver — limpeza e tipagem
│   └── load.py           # Camada Gold — tabelas analíticas + DuckDB
├── flows/
│   └── main_flow.py      # Orquestração com Prefect
├── app/
│   └── dashboard.py      # Dashboard Streamlit + Plotly
├── data/                 # Gerado automaticamente (ignorado pelo git)
│   ├── raw/
│   ├── trusted/
│   ├── refined/
│   └── ibge.duckdb
└── README.md
```
Tabelas no DuckDB:
```sql
SELECT * FROM pib_per_capita WHERE estado_sigla = 'RS' LIMIT 10;
SELECT * FROM ranking_estados ORDER BY ranking;
SELECT * FROM ipca_historico;

Insights dos dados:

Canaã dos Carajás (PA) lidera o PIB per capita municipal — sede da Mina S11D da Vale, maior mina de minério de ferro do mundo
DF lidera o PIB per capita estadual — menor território com alta concentração de servidores federais
SP tem o maior PIB total, representando ~30% da economia nacional
RS aparece entre os top 5 em PIB total e top 4 em PIB per capita médio estadual

Fonte dos dados:
API SIDRA — Sistema IBGE de Recuperação Automática. Dados públicos, sem necessidade de cadastro ou token.
