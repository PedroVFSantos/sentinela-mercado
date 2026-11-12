"""Sentinela de Mercado — dashboard Streamlit.

Hot path : receita por categoria lida do Redis (escrita pelo stream_processor.py).
Cold path: anomalias e estatísticas lidas das tabelas Delta gold (escritas pelo deep_analysis.py).

Configuração via variáveis de ambiente:
    REDIS_HOST / REDIS_PORT       (padrão: localhost / 6379)
    SENTINELA_DATA_DIR            (padrão: ./data ao lado deste arquivo)
    SENTINELA_REFRESH_SECONDS     (padrão: 2)
"""

import os
from pathlib import Path

import pandas as pd
import redis
import streamlit as st
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

# --- Configuração ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("SENTINELA_DATA_DIR", BASE_DIR / "data"))
ANOMALIES_PATH = DATA_DIR / "gold" / "anomalies"
INSIGHTS_PATH = DATA_DIR / "gold" / "market_insights"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REFRESH_SECONDS = float(os.getenv("SENTINELA_REFRESH_SECONDS", "2"))

st.set_page_config(page_title="Sentinela de Mercado", page_icon="🚨", layout="wide")


# --- Acesso a dados ---
@st.cache_resource
def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


def read_revenue() -> pd.DataFrame:
    """Lê as chaves revenue:<categoria> do Redis."""
    r = get_redis()
    keys = sorted(r.scan_iter(match="revenue:*"))
    if not keys:
        return pd.DataFrame(columns=["Categoria", "Receita ($)"])
    values = r.mget(keys)
    rows = [
        {"Categoria": key.split(":", 1)[1], "Receita ($)": float(value)}
        for key, value in zip(keys, values)
        if value is not None
    ]
    return pd.DataFrame(rows).sort_values("Receita ($)", ascending=False)


@st.cache_data(show_spinner=False, max_entries=8)
def _load_delta_version(path: str, version: int) -> pd.DataFrame:
    return DeltaTable(path, version=version).to_pandas()


def read_delta(path: Path) -> pd.DataFrame | None:
    """Lê uma tabela Delta, recarregando só quando surge uma versão nova.

    Retorna None se a tabela ainda não existe.
    """
    try:
        version = DeltaTable(str(path)).version()
    except TableNotFoundError:
        return None
    return _load_delta_version(str(path), version)


# --- Layout ---
st.title("📡 Sentinela de Mercado")
st.caption(f"Atualização automática a cada {REFRESH_SECONDS:g}s · Redis em {REDIS_HOST}:{REDIS_PORT} · dados em {DATA_DIR}")

tab_live, tab_alerts = st.tabs(["📈 Monitoramento em Tempo Real", "🚨 Alertas de Anomalias"])


@st.fragment(run_every=REFRESH_SECONDS)
def live_monitor() -> None:
    try:
        df = read_revenue()
    except redis.exceptions.ConnectionError:
        st.error(
            f"Não foi possível conectar ao Redis em {REDIS_HOST}:{REDIS_PORT}. "
            "A infraestrutura está de pé (`docker compose up -d`)?"
        )
        return

    if df.empty:
        st.info("Aguardando dados do Redis… Rode o producer e o `stream_processor.py`.")
        return

    c1, c2 = st.columns([1, 2])
    c1.metric("💰 Receita na janela atual (30s)", f"${df['Receita ($)'].sum():,.2f}")
    c1.metric("🏷️ Categorias ativas", len(df))
    c2.bar_chart(df, x="Categoria", y="Receita ($)", color="Categoria")


@st.fragment(run_every=REFRESH_SECONDS)
def anomaly_alerts() -> None:
    st.markdown(
        "Transações com **preço > média + 2σ** da própria categoria (Z-score > 2), "
        "calculadas pelo job batch `deep_analysis.py`."
    )

    anomalies = read_delta(ANOMALIES_PATH)
    if anomalies is None:
        st.info(
            f"Tabela `{ANOMALIES_PATH}` ainda não existe. "
            "Rode o `deep_analysis.py` para gerar a primeira análise."
        )
        return

    if anomalies.empty:
        st.success("✅ Nenhuma anomalia detectada no último processamento.")
    else:
        st.warning(f"⚠️ {len(anomalies)} transações suspeitas identificadas")
        st.dataframe(
            anomalies.sort_values("price", ascending=False),
            hide_index=True,
            column_config={
                "transaction_id": "Transação",
                "category": "Categoria",
                "price": st.column_config.NumberColumn("Preço", format="$%.2f"),
                "location": "Estado",
            },
        )

    insights = read_delta(INSIGHTS_PATH)
    if insights is not None and not insights.empty:
        st.subheader("📊 Estatísticas por categoria")
        st.dataframe(
            insights.sort_values("volume", ascending=False),
            hide_index=True,
            column_config={
                "category": "Categoria",
                "volume": st.column_config.NumberColumn("Volume", format="%d"),
                "preco_medio": st.column_config.NumberColumn("Preço médio", format="$%.2f"),
                "desvio_padrao": st.column_config.NumberColumn("Desvio padrão (σ)", format="$%.2f"),
                "preco_max": st.column_config.NumberColumn("Máximo", format="$%.2f"),
                "preco_min": st.column_config.NumberColumn("Mínimo", format="$%.2f"),
            },
        )


with tab_live:
    live_monitor()

with tab_alerts:
    anomaly_alerts()
