import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(page_title="IBGE Lakehouse", page_icon="🇧🇷", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Sans:wght@300;400;500&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    h1, h2, h3 { font-family: 'Syne', sans-serif !important; }
    .block-container { padding-top: 2rem; }
    .metric-card { background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); border: 1px solid #334155; border-radius: 12px; padding: 1.2rem 1.5rem; margin-bottom: 0.5rem; }
    .metric-label { color: #94a3b8; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.3rem; }
    .metric-value { color: #f1f5f9; font-size: 1.6rem; font-family: 'Syne', sans-serif; font-weight: 700; }
    .metric-sub { color: #38bdf8; font-size: 0.8rem; margin-top: 0.2rem; }
    .section-title { font-family: 'Syne', sans-serif; font-size: 1.1rem; font-weight: 700; color: #e2e8f0; border-left: 3px solid #38bdf8; padding-left: 0.75rem; margin: 1.5rem 0 1rem 0; }
    div[data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
</style>
""", unsafe_allow_html=True)

DUCKDB_PATH = Path("data/ibge.duckdb")

@st.cache_resource
def get_connection():
    if not DUCKDB_PATH.exists():
        st.error(f"Arquivo DuckDB não encontrado em: {DUCKDB_PATH}")
        st.stop()
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)

@st.cache_data
def query(sql: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(sql).fetchdf()

df_estados    = query("SELECT * FROM ranking_estados ORDER BY ranking")
df_municipios = query("SELECT * FROM pib_per_capita ORDER BY ranking_nacional")
df_ipca       = query("SELECT * FROM ipca_historico ORDER BY periodo")

estados_disponiveis = sorted(df_municipios["estado_sigla"].dropna().unique())

with st.sidebar:
    st.markdown("## 🇧🇷 IBGE Lakehouse")
    st.markdown("---")
    st.markdown("**Filtros**")
    estado_selecionado = st.selectbox("Estado", options=["Todos"] + estados_disponiveis,
        index=estados_disponiveis.index("RS") + 1 if "RS" in estados_disponiveis else 0)
    top_n = st.slider("Top N municípios", min_value=5, max_value=50, value=15, step=5)
    st.markdown("---")
    st.markdown("**Sobre o projeto**")
    st.caption("Pipeline de dados com arquitetura medallion (Bronze → Silver → Gold) usando API IBGE, DuckDB e Prefect.")
    st.caption("Dados: SIDRA/IBGE")

st.markdown("# 📊 IBGE Lakehouse")
st.markdown("Análise econômica municipal e estadual · Dados SIDRA/IBGE")
st.markdown("---")

total_municipios = len(df_municipios)
total_estados    = len(df_estados)
maior_pib_estado = df_estados.iloc[0]["estado_sigla"]
maior_pc_estado  = df_estados.sort_values("pib_per_capita_medio", ascending=False).iloc[0]["estado_sigla"]
pib_brasil       = df_estados["pib_total_mil_reais"].sum() / 1_000_000

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Municípios analisados</div><div class="metric-value">{total_municipios:,}</div><div class="metric-sub">{total_estados} estados</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-card"><div class="metric-label">PIB Brasil (estimado)</div><div class="metric-value">R$ {pib_brasil:.1f} bi</div><div class="metric-sub">em R$ milhões</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Maior PIB total</div><div class="metric-value">{maior_pib_estado}</div><div class="metric-sub">estado com maior economia</div></div>', unsafe_allow_html=True)
with col4:
    st.markdown(f'<div class="metric-card"><div class="metric-label">Maior PIB per capita</div><div class="metric-value">{maior_pc_estado}</div><div class="metric-sub">média dos municípios</div></div>', unsafe_allow_html=True)

st.markdown('<div class="section-title">Ranking de Estados</div>', unsafe_allow_html=True)
col_a, col_b = st.columns(2)

with col_a:
    fig1 = px.bar(df_estados.head(10), x="pib_total_mil_reais", y="estado_sigla", orientation="h",
        title="PIB Total por Estado (Top 10)", labels={"pib_total_mil_reais": "PIB (R$ mil)", "estado_sigla": "Estado"},
        color="pib_total_mil_reais", color_continuous_scale="Blues")
    fig1.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a", font_color="#e2e8f0",
        coloraxis_showscale=False, yaxis={"categoryorder": "total ascending"}, title_font_family="Syne")
    st.plotly_chart(fig1, use_container_width=True)

with col_b:
    df_pc = df_estados.sort_values("pib_per_capita_medio", ascending=False).head(10)
    fig2 = px.bar(df_pc, x="pib_per_capita_medio", y="estado_sigla", orientation="h",
        title="PIB per Capita Médio por Estado (Top 10)", labels={"pib_per_capita_medio": "PIB per capita (R$)", "estado_sigla": "Estado"},
        color="pib_per_capita_medio", color_continuous_scale="Teal")
    fig2.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a", font_color="#e2e8f0",
        coloraxis_showscale=False, yaxis={"categoryorder": "total ascending"}, title_font_family="Syne")
    st.plotly_chart(fig2, use_container_width=True)

st.markdown('<div class="section-title">Municípios</div>', unsafe_allow_html=True)

if estado_selecionado == "Todos":
    df_mun_filtrado = df_municipios.head(top_n)
    titulo_mun = f"Top {top_n} Municípios — Brasil"
else:
    df_mun_filtrado = df_municipios[df_municipios["estado_sigla"] == estado_selecionado].head(top_n)
    titulo_mun = f"Top {top_n} Municípios — {estado_selecionado}"

col_c, col_d = st.columns([3, 2])
with col_c:
    fig3 = px.bar(df_mun_filtrado.sort_values("pib_per_capita"), x="pib_per_capita", y="municipio_nome", orientation="h",
        title=titulo_mun + " · PIB per Capita", labels={"pib_per_capita": "PIB per capita (R$)", "municipio_nome": ""},
        color="pib_per_capita", color_continuous_scale="Oranges")
    fig3.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a", font_color="#e2e8f0",
        coloraxis_showscale=False, height=500, title_font_family="Syne")
    st.plotly_chart(fig3, use_container_width=True)

with col_d:
    st.markdown(f"**{titulo_mun}**")
    st.dataframe(
        df_mun_filtrado[["ranking_nacional", "municipio_nome", "estado_sigla", "populacao_estimada", "pib_per_capita"]]
        .rename(columns={"ranking_nacional": "Rank", "municipio_nome": "Município", "estado_sigla": "UF",
                         "populacao_estimada": "População", "pib_per_capita": "PIB per capita (R$)"})
        .reset_index(drop=True),
        use_container_width=True, height=460)

st.markdown('<div class="section-title">PIB vs População por Município</div>', unsafe_allow_html=True)
df_scatter = df_municipios if estado_selecionado == "Todos" else df_municipios[df_municipios["estado_sigla"] == estado_selecionado]
fig4 = px.scatter(df_scatter, x="populacao_estimada", y="pib_mil_reais",
    color="estado_sigla" if estado_selecionado == "Todos" else "pib_per_capita",
    hover_name="municipio_nome", hover_data={"estado_sigla": True, "pib_per_capita": ":,.0f"},
    title="Relação entre População e PIB Municipal",
    labels={"populacao_estimada": "População Estimada", "pib_mil_reais": "PIB (R$ mil)", "estado_sigla": "Estado"},
    opacity=0.7, size_max=20)
fig4.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a", font_color="#e2e8f0", title_font_family="Syne", height=420)
st.plotly_chart(fig4, use_container_width=True)

st.markdown('<div class="section-title">IPCA — Histórico de Inflação</div>', unsafe_allow_html=True)
col_e, col_f = st.columns(2)

with col_e:
    fig5 = px.bar(df_ipca, x=df_ipca["periodo"].astype(str), y="variacao_pct",
        title="Variação IPCA por Período (%)", labels={"x": "Período", "variacao_pct": "Variação (%)"},
        color="variacao_pct", color_continuous_scale="RdYlGn_r")
    fig5.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a", font_color="#e2e8f0",
        coloraxis_showscale=False, title_font_family="Syne")
    st.plotly_chart(fig5, use_container_width=True)

with col_f:
    fig6 = px.line(df_ipca, x=df_ipca["periodo"].astype(str), y="variacao_acumulada_pct",
        title="Variação Acumulada IPCA (%)", labels={"x": "Período", "variacao_acumulada_pct": "Acumulado (%)"},
        markers=True)
    fig6.update_traces(line_color="#38bdf8", marker_color="#38bdf8")
    fig6.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a", font_color="#e2e8f0", title_font_family="Syne")
    st.plotly_chart(fig6, use_container_width=True)

st.markdown("---")
st.caption("Projeto: IBGE Lakehouse · Pipeline: Prefect · Storage: DuckDB + Parquet · Fonte: SIDRA/IBGE")
