import streamlit as st
import redis
import pandas as pd
import time
from deltalake import DeltaTable

# --- Configuração da Página ---
st.set_page_config(page_title="Sentinela Real-Time & Analytics", page_icon="🚨", layout="wide")

# Caminhos dos Dados no seu Arch
DELTA_PATH = "/home/mancopc/sentinela-mercado/data/gold/anomalies"

# --- Conexão Redis ---
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

st.title("📡 Sentinela de Mercado v2.0")

# Criando as Abas
tab1, tab2 = st.tabs(["📈 Monitoramento em Tempo Real", "🚨 Alertas de Fraude (Anomalias)"])

# --- ABA 1: MONITORAMENTO ---
with tab1:
    placeholder = st.empty()

# --- ABA 2: ALERTAS DE FRAUDE ---
with tab2:
    st.header("🕵️ Detecção de Anomalias Estatísticas")
    st.info("As anomalias abaixo foram detectadas pelo motor Spark via cálculo de Z-Score (Preço > Média + 2σ).")
    placeholder_fraude = st.empty()

# --- Loop de Atualização ---
while True:
    # Atualizando Monitoramento (Hot Path - Redis)
    with placeholder.container():
        keys = r.keys("revenue:*")
        if keys:
            data = []
            values = r.mget(keys)
            for k, v in zip(keys, values):
                data.append({"Categoria": k.split(":")[1], "Receita ($)": float(v)})
            df_redis = pd.DataFrame(data).sort_values(by="Receita ($)", ascending=False)
            
            c1, c2 = st.columns([1, 2])
            c1.metric("💰 Receita Total", f"${df_redis['Receita ($)'].sum():,.2f}")
            c2.bar_chart(df_redis, x="Categoria", y="Receita ($)", color="Categoria")
        else:
            st.warning("Aguardando dados do Redis...")

    # Atualizando Alertas (Cold Path - Delta Lake)
    with placeholder_fraude.container():
        try:
            # Lendo a tabela Delta diretamente do disco
            dt = DeltaTable(DELTA_PATH)
            df_anomalias = dt.to_pandas()
            
            if not df_anomalias.empty:
                st.warning(f"⚠️ {len(df_anomalias)} Transações suspeitas identificadas!")
                st.dataframe(df_anomalias.sort_values(by="price", ascending=False), use_container_width=True)
            else:
                st.success("✅ Nenhuma anomalia detectada no último processamento.")
        except Exception:
            st.info("Aguardando primeiro processamento do script 'deep_analysis.py'...")

    time.sleep(2)
