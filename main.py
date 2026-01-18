import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(layout="wide")

# Ajuste aqui para o nome/caminho do CSV que você gerou
CSV_PATH = "wait_times_summary_global_0700_2300_with_park_id.csv"

@st.cache_data(show_spinner="Loading CSV...")
def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Normaliza tipos/strings
    for c in ["park_name", "land", "ride_name"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # hora como string "HH:MM" (mantém compatível com o gráfico)
    df["hora"] = df["hora"].astype(str).str.slice(0, 5)

    # Ordenação por hora correta
    df["hora_dt"] = pd.to_datetime(df["hora"], format="%H:%M", errors="coerce")

    return df

@st.cache_data(show_spinner=False)
def get_parks_from_df(df_all: pd.DataFrame) -> list[str]:
    return sorted([p for p in df_all["park_name"].dropna().unique().tolist() if str(p).strip()])

@st.cache_data(show_spinner=False)
def get_rides_for_park_from_df(df_all: pd.DataFrame, park_name: str) -> list[str]:
    rides = df_all.loc[df_all["park_name"] == park_name, "ride_name"].dropna().unique().tolist()
    rides = [r for r in rides if str(r).strip()]
    return sorted(rides)

st.title("Ride wait times (30-min resolution)")

# -----------------------------
# Carrega CSV global (sem SQLite)
# -----------------------------
df_all = load_csv(CSV_PATH)

if df_all.empty:
    st.error("O CSV está vazio ou não foi possível carregar.")
    st.stop()

required_cols = {
    "park_name", "ride_name", "hora",
    "avg_wait", "min_wait", "max_wait", "p25_wait", "p75_wait", "n_samples"
}
missing = required_cols - set(df_all.columns)
if missing:
    st.error(f"CSV não tem colunas esperadas: {sorted(missing)}")
    st.stop()

# -----------------------------
# Sidebar (filtros) - igual UX
# -----------------------------
with st.sidebar:
    st.header("Filtros")

    parks = get_parks_from_df(df_all)
    if not parks:
        st.error("Nenhum park_name encontrado no CSV.")
        st.stop()

    park_selected = st.selectbox("Park", options=parks, index=0)

    rides = get_rides_for_park_from_df(df_all, park_selected)
    if not rides:
        st.warning("Nenhum ride encontrado para este park no CSV.")
        st.stop()

    ride_filter_text = st.text_input("Filtrar lista de rides (opcional)", value="").strip().lower()
    ride_options = [r for r in rides if ride_filter_text in r.lower()] if ride_filter_text else rides
    if not ride_options:
        st.warning("Nenhum ride combina com o filtro. Limpando filtro...")
        ride_options = rides

    ride_selected = st.selectbox("Ride", options=ride_options, index=0)

    # No CSV, isso já está “global is_open=1” (do jeito que você gerou).
    # Mantive o checkbox pra UI ficar igual, mas ele não altera nada.
    st.checkbox("Somente quando is_open = 1", value=True, disabled=True)

    limit_rows = st.number_input(
        "Limite de linhas (resultado final)",
        min_value=50,
        max_value=5000,
        value=1000,
        step=50
    )

# -----------------------------
# Filtra do CSV (equivalente ao SQL)
# -----------------------------
df = df_all[(df_all["park_name"] == park_selected) & (df_all["ride_name"] == ride_selected)].copy()

if df.empty:
    st.warning("Nenhum dado encontrado para o park/ride selecionados no CSV.")
    st.stop()

# Ordenação e limite (igual antes)
df = df.sort_values(["park_name", "land", "ride_name", "hora_dt"])
df = df.head(int(limit_rows))
df["hora_str"] = df["hora_dt"].dt.strftime("%H:%M")

st.subheader("Dados agregados (07:00–23:00) — do CSV")
st.dataframe(df, use_container_width=True)

# -----------------------------
# Banda P25–P75 (SEM empilhar)
# -----------------------------
df_band_wide = df[["ride_name", "hora_str", "p75_wait", "p25_wait"]].copy()

fig_band = px.line(
    df_band_wide,
    x="hora_str",
    y=["p75_wait", "p25_wait"],  # ordem importa
    facet_row="ride_name",
    title=f"Fila ao longo do dia (07:00–23:00) — {park_selected} / {ride_selected}",
)

for t in fig_band.data:
    if t.name == "p75_wait":
        t.update(mode="lines", line=dict(width=0), showlegend=False)
    elif t.name == "p25_wait":
        t.update(mode="lines", line=dict(width=0), fill="tonexty", name="P25–P75")

# -----------------------------
# Linhas: média, min, max
# -----------------------------
df_lines = df.melt(
    id_vars=["ride_name", "hora_str"],
    value_vars=["avg_wait", "min_wait", "max_wait"],
    var_name="estatistica",
    value_name="wait",
)

label_map = {"avg_wait": "Média", "min_wait": "Mínimo", "max_wait": "Máximo"}
df_lines["estatistica"] = df_lines["estatistica"].map(label_map).fillna(df_lines["estatistica"])

fig_lines = px.line(
    df_lines,
    x="hora_str",
    y="wait",
    color="estatistica",
    facet_row="ride_name",
    markers=True,
)

# -----------------------------
# Combinar
# -----------------------------
fig = fig_band
for trace in fig_lines.data:
    fig.add_trace(trace)

fig.update_layout(
    xaxis_title="Hora",
    yaxis_title="Wait (min)",
    hovermode="x unified",
)

st.plotly_chart(fig, use_container_width=True)
