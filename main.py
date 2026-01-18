import pandas as pd
import streamlit as st
import plotly.express as px
import requests
from datetime import datetime, timezone

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

CSV_PATH = "wait_times_summary_global_0700_2300_with_park_id.csv"
PARKS_URL = "https://queue-times.com/parks.json"
REQUEST_TIMEOUT = 20

# -----------------------------
# CSV (histórico agregados)
# -----------------------------
@st.cache_data(show_spinner="Loading CSV...")
def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    for c in ["park_name", "land", "ride_name"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    df["hora"] = df["hora"].astype(str).str.slice(0, 5)
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

@st.cache_data(show_spinner=False)
def get_park_id_from_csv(df_all: pd.DataFrame, park_name: str) -> int | None:
    ids = df_all.loc[df_all["park_name"] == park_name, "park_id"].dropna().unique().tolist()
    if not ids:
        return None
    try:
        return int(ids[0])
    except Exception:
        return None

# -----------------------------
# LIVE API (por park_id) - separado e depois juntado
# -----------------------------
@st.cache_data(ttl=60, show_spinner="Fetching parks list...")
def fetch_parks_list() -> list[dict]:
    r = requests.get(PARKS_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=60, show_spinner=False)
def get_park_name_by_id(park_id: int) -> str | None:
    # Cacheia o lookup park_id -> park_name
    data = fetch_parks_list()
    for dest in data:
        for p in (dest.get("parks") or []):
            if p.get("id") == park_id:
                return p.get("name")
    return None

@st.cache_data(ttl=60, show_spinner="Fetching live queue times for this park...")
def fetch_live_queues_for_park(park_id: int) -> dict:
    url = f"https://queue-times.com/parks/{park_id}/queue_times.json"
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=60, show_spinner=False)
def build_live_dfs_for_park(park_id: int) -> dict:
    """
    Retorna 3 dataframes em cache (por park_id):
      - df_lands: lista de lands do parque
      - df_rides: rides (1 linha por ride) com wait_min, is_open, status
      - df_join: df_rides enriquecido com info de park (park_id, park_name)
    """
    park_name = get_park_name_by_id(park_id)
    payload = fetch_live_queues_for_park(park_id)

    ts_utc = datetime.now(timezone.utc).isoformat()

    lands_rows = []
    rides_rows = []

    for land in (payload.get("lands") or []):
        land_name = land.get("name")
        lands_rows.append({"park_id": park_id, "land": land_name})

        for ride in (land.get("rides") or []):
            rides_rows.append({
                "ts_utc": ts_utc,
                "park_id": park_id,
                "land": land_name,
                "ride_id": ride.get("id"),
                "ride_name": ride.get("name"),
                "wait_min": ride.get("wait_time"),
                "is_open": 1 if ride.get("is_open") else 0,
                "status": ride.get("status"),
                "last_updated": ride.get("last_updated"),
            })

    df_lands = pd.DataFrame(lands_rows).drop_duplicates()
    df_rides = pd.DataFrame(rides_rows)

    if not df_rides.empty:
        df_rides["wait_min"] = pd.to_numeric(df_rides["wait_min"], errors="coerce")
        df_rides["is_open"] = pd.to_numeric(df_rides["is_open"], errors="coerce")
        for col in ["land", "ride_name", "status"]:
            if col in df_rides.columns:
                df_rides[col] = df_rides[col].astype("string").str.strip()

    # df separado com info do parque (pra join)
    df_park = pd.DataFrame([{
        "park_id": park_id,
        "park_name": park_name if park_name else None,
    }])

    # join final (o que você quer usar depois)
    df_join = df_rides.merge(df_park, on="park_id", how="left")

    return {"df_lands": df_lands, "df_rides": df_rides, "df_join": df_join}

# -----------------------------
# App
# -----------------------------
st.title("Ride wait times")

df_all = load_csv(CSV_PATH)

required_cols = {
    "park_id", "park_name", "ride_name", "hora",
    "avg_wait", "min_wait", "max_wait", "p25_wait", "p75_wait", "n_samples"
}
missing = required_cols - set(df_all.columns)
if missing:
    st.error(f"CSV não tem colunas esperadas: {sorted(missing)}")
    st.stop()

parks = get_parks_from_df(df_all)
if not parks:
    st.error("Nenhum park_name encontrado no CSV.")
    st.stop()

with st.sidebar:
    st.header("Filtros")
    park_selected = st.selectbox("Park", options=parks, index=0)

    rides = get_rides_for_park_from_df(df_all, park_selected)
    ride_filter_text = st.text_input("Filtrar rides (opcional)", value="").strip().lower()
    ride_options = [r for r in rides if ride_filter_text in r.lower()] if ride_filter_text else rides
    if not ride_options:
        ride_options = rides
    ride_selected = st.selectbox("Ride", options=ride_options, index=0)

    limit_rows = st.slider("Limite de linhas", min_value=50, max_value=2000, value=500, step=50)

# -----------------------------
# LIVE (por park_id) em cache
# -----------------------------
park_id_selected = get_park_id_from_csv(df_all, park_selected)
if park_id_selected is None:
    st.warning("Não encontrei park_id no CSV para esse park_name, então não dá pra puxar a fila ao vivo por park_id.")
else:
    live = build_live_dfs_for_park(park_id_selected)
    df_live_join = live["df_join"]   # <- este é o DF que você vai usar depois
    df_live_rides = live["df_rides"]
    df_live_ride_now = df_live_join[df_live_join["ride_name"] == ride_selected].copy()

    with st.expander("📡 Fila agora (ao vivo)", expanded=True):
        c1, c2 = st.columns([1, 1])

        with c1:
            if df_live_ride_now.empty:
                st.warning("Não achei esse ride no retorno ao vivo.")
            else:
                r0 = df_live_ride_now.iloc[0]
                wait_now = r0.get("wait_min")
                is_open_now = r0.get("is_open")
                st.metric("Wait agora (min)", value="—" if pd.isna(wait_now) else int(wait_now))
                st.write(f"**Aberto agora?** {'Sim' if int(is_open_now) == 1 else 'Não'}")
                if pd.notna(r0.get("last_updated")):
                    st.caption(f"Última atualização (API): {r0.get('last_updated')}")

        with c2:
            st.write("Top 10 filas (ao vivo)")
            top = (
                df_live_join.dropna(subset=["wait_min"])
                .sort_values("wait_min", ascending=False)
                .head(10)[["ride_name", "land", "wait_min", "is_open"]]
            )
            st.dataframe(top, use_container_width=True, height=260)

        # Opcional: deixa disponível pra inspeção/uso posterior
        st.caption("DataFrame ao vivo (df_live_join) — em cache (ttl=60s)")
        st.dataframe(df_live_join.head(50), use_container_width=True)

# -----------------------------
# HISTÓRICO (CSV) + gráfico (igual ao que você tinha)
# -----------------------------
df = df_all[(df_all["park_name"] == park_selected) & (df_all["ride_name"] == ride_selected)].copy()
if df.empty:
    st.warning("Nenhum dado encontrado para o park/ride selecionados no CSV.")
    st.stop()

df = df.sort_values(["park_name", "land", "ride_name", "hora_dt"]).head(int(limit_rows))
df["hora_str"] = df["hora_dt"].dt.strftime("%H:%M")

st.subheader("Histórico agregado (07:00–23:00)")
st.dataframe(df, use_container_width=True, height=240)

# Banda P25–P75 (SEM empilhar)
df_band_wide = df[["ride_name", "hora_str", "p75_wait", "p25_wait"]].copy()
fig_band = px.line(
    df_band_wide,
    x="hora_str",
    y=["p75_wait", "p25_wait"],
    facet_row="ride_name",
    title=f"Fila ao longo do dia (07:00–23:00) — {park_selected} / {ride_selected}",
)
for t in fig_band.data:
    if t.name == "p75_wait":
        t.update(mode="lines", line=dict(width=0), showlegend=False)
    elif t.name == "p25_wait":
        t.update(mode="lines", line=dict(width=0), fill="tonexty", name="P25–P75")

# Linhas: média, min, max
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

fig = fig_band
for trace in fig_lines.data:
    fig.add_trace(trace)

fig.update_layout(
    xaxis_title="Hora",
    yaxis_title="Wait (min)",
    hovermode="x unified",
)

st.plotly_chart(fig, use_container_width=True)
