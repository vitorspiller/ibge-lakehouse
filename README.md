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


Insights dos dados
Canaã dos Carajás (PA) lidera o PIB per capita municipal — sede da Mina S11D da Vale, maior mina de minério de ferro do mundo
DF lidera o PIB per capita estadual — menor território com alta concentração de servidores federais
SP tem o maior PIB total, representando ~30% da economia nacional
RS aparece entre os top 5 em PIB total e top 4 em PIB per capita médio estadual

Fonte dos dados
API SIDRA — Sistema IBGE de Recuperação Automática. Dados públicos, sem necessidade de cadastro ou token.
