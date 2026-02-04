import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Finance Tracker", layout="wide")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LEDGER_CSV = PROJECT_ROOT / "data" / "processed" / "ledger.csv"

st.title("Finance Tracker — Nubank + Itaú")
st.caption("Dashboard local. Seus dados ficam no seu computador.")

if not LEDGER_CSV.exists():
    st.error("Não encontrei data/processed/ledger.csv. Rode os importadores primeiro.")
    st.stop()

@st.cache_data
def load_ledger(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"tx_id": "string", "external_id": "string"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["description"] = df["description"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip().str.lower()
    df["type"] = df["type"].astype(str).str.strip().str.lower()

    # garante colunas opcionais
    for col in ["category", "subcategory"]:
        if col not in df.columns:
            df[col] = "Uncategorized"

    # mês (YYYY-MM)
    df["month"] = df["date"].dt.to_period("M").astype(str)

    # uma versão "limpa" curta da descrição (ajuda top gastos)
    df["desc_short"] = (
        df["description"]
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 60)
    )
    return df.dropna(subset=["date", "amount"])

df = load_ledger(LEDGER_CSV)

# ---------------- Sidebar filtros ----------------
st.sidebar.header("Filtros")

min_date = df["date"].min().date()
max_date = df["date"].max().date()

date_range = st.sidebar.date_input(
    "Período",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

sources = sorted(df["source"].dropna().unique().tolist())
sel_sources = st.sidebar.multiselect("Fontes", sources, default=sources)

types = sorted(df["type"].dropna().unique().tolist())
sel_types = st.sidebar.multiselect("Tipos", types, default=types)

text_filter = st.sidebar.text_input("Buscar na descrição (contém)", value="").strip().lower()

# aplica filtros
f = df[
    (df["date"].dt.date >= start_date) &
    (df["date"].dt.date <= end_date) &
    (df["source"].isin(sel_sources)) &
    (df["type"].isin(sel_types))
].copy()

if text_filter:
    f = f[f["description"].str.lower().str.contains(text_filter, na=False)]

# ---------------- KPIs ----------------
total_expense = f.loc[f["amount"] < 0, "amount"].sum()
total_income = f.loc[f["amount"] > 0, "amount"].sum()
net = total_income + total_expense  # expense já é negativo

def brl(x: float) -> str:
    s = f"{x:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Gastos (período)", brl(total_expense))
c2.metric("Recebimentos (período)", brl(total_income))
c3.metric("Saldo líquido (período)", brl(net))
c4.metric("Transações (filtradas)", f"{len(f)}")

st.divider()

# ---------------- Gráficos ----------------
g1, g2 = st.columns(2)

with g1:
    st.subheader("Evolução mensal (saldo, gastos, recebimentos)")
    by_month = f.groupby("month", as_index=False).agg(
        gastos=("amount", lambda s: s[s < 0].sum()),
        receb=("amount", lambda s: s[s > 0].sum()),
        saldo=("amount", "sum"),
    )
    fig = px.line(by_month, x="month", y=["gastos", "receb", "saldo"])
    st.plotly_chart(fig, use_container_width=True)

with g2:
    st.subheader("Gastos por fonte")
    by_source = f.loc[f["amount"] < 0].groupby("source", as_index=False)["amount"].sum()
    fig2 = px.bar(by_source, x="source", y="amount")
    st.plotly_chart(fig2, use_container_width=True)

g3, g4 = st.columns(2)

st.subheader("Gastos por categoria (somente expense)")
gastos = f[(f["type"] == "expense") & (f["amount"] < 0)].copy()

by_cat = (
    gastos.groupby("category", as_index=False)["amount"]
    .sum()
    .sort_values("amount")
)
fig_cat = px.bar(by_cat, x="amount", y="category", orientation="h")
st.plotly_chart(fig_cat, use_container_width=True)

st.caption(f"Linhas consideradas no gráfico: {len(gastos)}")


with g3:
    st.subheader("Top descrições (gastos)")
    top = (
        f.loc[f["amount"] < 0]
        .groupby("desc_short", as_index=False)["amount"]
        .sum()
        .sort_values("amount")
        .head(15)
    )
    fig3 = px.bar(top, x="amount", y="desc_short", orientation="h")
    st.plotly_chart(fig3, use_container_width=True)

with g4:
    st.subheader("Distribuição por tipo")
    t = f.groupby("type", as_index=False)["amount"].sum()
    fig4 = px.bar(t, x="type", y="amount")
    st.plotly_chart(fig4, use_container_width=True)

    st.subheader("Transferências (saídas)")
    tr = f[(f["type"] == "transfer") & (f["amount"] < 0)].copy()
    by_tr = tr.groupby("category", as_index=False)["amount"].sum().sort_values("amount")
    fig_tr = px.bar(by_tr, x="amount", y="category", orientation="h")
    st.plotly_chart(fig_tr, use_container_width=True)


st.divider()

# ---------------- Tabela + export ----------------
st.subheader("Transações (filtradas)")
show_cols = ["date", "source", "type", "description", "amount", "category", "subcategory", "external_id", "tx_id", "file_name"]
show_cols = [c for c in show_cols if c in f.columns]

st.dataframe(
    f.sort_values("date", ascending=False)[show_cols],
    use_container_width=True,
    height=450,
)

csv_bytes = f.sort_values("date", ascending=False).to_csv(index=False, encoding="utf-8").encode("utf-8")
st.download_button(
    "Baixar CSV (filtrado)",
    data=csv_bytes,
    file_name="ledger_filtrado.csv",
    mime="text/csv"
)

