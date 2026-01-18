import pandas as pd
import streamlit as st
import plotly.express as px
import requests
from datetime import datetime, timezone

# Mobile-friendly
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

CSV_PATH = "wait_times_summary_global_0700_2300_with_park_id.csv"
PARKS_URL = "https://queue-times.com/parks.json"
REQUEST_TIMEOUT = 20

# -----------------------------
# Helpers
# -----------------------------
def floor_to_30min_hhmm(hhmm: str) -> str:
    """
    Converte "HH:MM" para bucket de 30 min:
      11:27 -> 11:00
      11:31 -> 11:30
    """
    try:
        h, m = hhmm.split(":")
        h = int(h)
        m = int(m)
        m_bucket = 0 if m < 30 else 30
        return f"{h:02d}:{m_bucket:02d}"
    except Exception:
        return hhmm[:5]

def classify_wait(wait_now, avg, p25, p75, tol=2):
    """
    Regras:
      - wait_now > p75  -> Muito ruim
      - avg < wait_now <= p75 -> Ruim
      - wait_now == avg (± tol) -> Médio
      - p25 < wait_now < avg -> Bom
      - wait_now <= p25 -> Muito bom
    """
    if pd.isna(wait_now) or pd.isna(avg) or pd.isna(p25) or pd.isna(p75):
        return "Sem dados", "⚪"

    if wait_now > p75:
        return "Muito ruim", "🔴"
    elif wait_now > avg:
        return "Ruim", "🟠"
    elif abs(wait_now - avg) <= tol:
        return "Médio", "🟡"
    elif wait_now > p25:
        return "Bom", "🟢"
    else:
        return "Muito bom", "🟢🟢"

# -----------------------------
# CSV (histórico agregados)
# -----------------------------
@st.cache_data(show_spinner="Loading CSV...")
def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    for c in ["park_name", "land", "ride_name"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # eixo X do histórico é categórico "HH:MM"
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
    Retorna DFs em cache (por park_id):
      - df_rides: rides com wait_min, is_open, status + ts_utc + ts_florida + hora_florida
      - df_join: df_rides enriquecido com park_name
    """
    park_name = get_park_name_by_id(park_id)
    payload = fetch_live_queues_for_park(park_id)

    ts_utc_iso = datetime.now(timezone.utc).isoformat()
    ts_florida_dt = (
        pd.to_datetime(ts_utc_iso, utc=True)
        .tz_convert("Etc/GMT+5")   # UTC-5 fixo
        .tz_localize(None)
    )

    rides_rows = []
    for land in (payload.get("lands") or []):
        land_name = land.get("name")
        for ride in (land.get("rides") or []):
            hhmm = ts_florida_dt.strftime("%H:%M")
            rides_rows.append({
                "ts_utc": ts_utc_iso,
                "ts_florida": ts_florida_dt,
                "hora_florida": hhmm,
                "hora_florida_bucket": floor_to_30min_hhmm(hhmm),
                "park_id": park_id,
                "land": land_name,
                "ride_id": ride.get("id"),
                "ride_name": ride.get("name"),
                "wait_min": ride.get("wait_time"),
                "is_open": 1 if ride.get("is_open") else 0,
                "status": ride.get("status"),
                "last_updated": ride.get("last_updated"),
            })

    df_rides = pd.DataFrame(rides_rows)
    if not df_rides.empty:
        df_rides["wait_min"] = pd.to_numeric(df_rides["wait_min"], errors="coerce")
        df_rides["is_open"] = pd.to_numeric(df_rides["is_open"], errors="coerce")
        for col in ["land", "ride_name", "status"]:
            if col in df_rides.columns:
                df_rides[col] = df_rides[col].astype("string").str.strip()

    df_park = pd.DataFrame([{"park_id": park_id, "park_name": park_name if park_name else None}])
    df_join = df_rides.merge(df_park, on="park_id", how="left")
    return {"df_rides": df_rides, "df_join": df_join}

# -----------------------------
# App
# -----------------------------
st.title("Ride wait times")

df_all = load_csv(CSV_PATH)
missing = {"park_id","park_name","ride_name","hora","avg_wait","min_wait","max_wait","p25_wait","p75_wait","n_samples"} - set(df_all.columns)
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
    if not rides:
        st.warning("Nenhum ride encontrado para este park no CSV.")
        st.stop()

    ride_filter_text = st.text_input("Filtrar rides (opcional)", value="").strip().lower()
    ride_options = [r for r in rides if ride_filter_text in r.lower()] if ride_filter_text else rides
    if not ride_options:
        ride_options = rides

    ride_selected = st.selectbox("Ride", options=ride_options, index=0)

    limit_rows = st.slider("Limite de linhas", min_value=50, max_value=2000, value=500, step=50)

    st.divider()
    st.caption("Classificação: >P75=muito ruim; avg–P75=ruim; ~=avg=médio; P25–avg=bom; <=P25=muito bom.")

# -----------------------------
# HISTÓRICO (CSV) do ride selecionado
# -----------------------------
df = df_all[(df_all["park_name"] == park_selected) & (df_all["ride_name"] == ride_selected)].copy()
if df.empty:
    st.warning("Nenhum dado encontrado para o park/ride selecionados no CSV.")
    st.stop()

df = df.sort_values(["park_name", "land", "ride_name", "hora_dt"]).head(int(limit_rows))
df["hora_str"] = df["hora_dt"].dt.strftime("%H:%M")

# -----------------------------
# LIVE (por park_id) em cache + classificação geral
# -----------------------------
df_live_join = pd.DataFrame()
df_live_ride_now = pd.DataFrame()
df_class = None  # classificação de todos os rides do parque (ao vivo)

park_id_selected = get_park_id_from_csv(df_all, park_selected)
if park_id_selected is not None:
    live = build_live_dfs_for_park(park_id_selected)
    df_live_join = live["df_join"]
    df_live_ride_now = df_live_join[df_live_join["ride_name"] == ride_selected].copy()

    # baseline do CSV por ride (no parque selecionado)
    baseline = (
        df_all[df_all["park_name"] == park_selected]
        .groupby("ride_name", as_index=False)
        .agg(
            avg_wait=("avg_wait", "mean"),
            p25_wait=("p25_wait", "mean"),
            p75_wait=("p75_wait", "mean"),
        )
    )

    # junta live + baseline e classifica
    df_class = df_live_join.merge(baseline, on="ride_name", how="left")
    df_class["classificacao"] = df_class.apply(
        lambda r: classify_wait(r.get("wait_min"), r.get("avg_wait"), r.get("p25_wait"), r.get("p75_wait"))[0],
        axis=1
    )
    df_class["icon"] = df_class.apply(
        lambda r: classify_wait(r.get("wait_min"), r.get("avg_wait"), r.get("p25_wait"), r.get("p75_wait"))[1],
        axis=1
    )

with st.expander("📡 Fila agora (ao vivo) + classificação do parque", expanded=True):
    if park_id_selected is None:
        st.warning("Não encontrei park_id no CSV para esse park_name, então não dá pra puxar a fila ao vivo por park_id.")
    else:
        c1, c2 = st.columns([1, 1])

        with c1:
            if df_live_ride_now.empty:
                st.warning("Não achei esse ride no retorno ao vivo.")
            else:
                r0 = df_live_ride_now.iloc[0]
                wait_now = r0.get("wait_min")
                hhmm = r0.get("hora_florida")
                bucket = r0.get("hora_florida_bucket")

                st.metric("Wait agora (min)", value="—" if pd.isna(wait_now) else int(wait_now))
                st.write(f"**Hora Florida (UTC-5):** {hhmm}  |  **Bucket:** {bucket}")
                st.write(f"**Aberto agora?** {'Sim' if int(r0.get('is_open', 0)) == 1 else 'Não'}")
                if pd.notna(r0.get("last_updated")):
                    st.caption(f"Última atualização (API): {r0.get('last_updated')}")

                # classificação do ride selecionado (usando baseline do CSV)
                if df_class is not None and not df_class.empty:
                    rr = df_class[df_class["ride_name"] == ride_selected]
                    if not rr.empty:
                        rr0 = rr.iloc[0]
                        label, icon = classify_wait(
                            rr0.get("wait_min"), rr0.get("avg_wait"), rr0.get("p25_wait"), rr0.get("p75_wait")
                        )
                        st.markdown(f"### {icon} {label}")

        with c2:
            st.subheader("Rides com pior fila agora")
            if df_class is None or df_class.empty:
                st.info("Classificação indisponível.")
            else:
                # ordena por pior (wait maior)
                df_show = (
                    df_class[["icon", "ride_name", "land", "wait_min", "classificacao"]]
                    .dropna(subset=["wait_min"])
                    .sort_values("wait_min", ascending=False)
                    .head(20)
                )
                st.dataframe(df_show, use_container_width=True, height=380)

# -----------------------------
# HISTÓRICO + gráfico (igual ao que você tinha) + PONTO AGORA
# -----------------------------
#st.subheader("Histórico agregado (07:00–23:00)")
#st.dataframe(df, use_container_width=True, height=240)

# Banda P25–P75
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

# Combinar
fig = fig_band
for trace in fig_lines.data:
    fig.add_trace(trace)

# PONTO "AGORA" alinhado ao bucket de 30 min do eixo
if not df_live_ride_now.empty:
    r0 = df_live_ride_now.iloc[0]
    wait_now = r0.get("wait_min")
    bucket = r0.get("hora_florida_bucket")  # ex: 11:27 -> 11:00

    # Só plota se o bucket existir no eixo do histórico
    if pd.notna(wait_now) and isinstance(bucket, str) and bucket in set(df["hora_str"].unique()):
        df_now_point = pd.DataFrame([{
            "hora_str": bucket,
            "wait": float(wait_now),
            "label": "AGORA",
        }])

        fig_now = px.scatter(df_now_point, x="hora_str", y="wait", text="label")
        for t in fig_now.data:
            t.update(
                marker=dict(size=16, color="red", symbol="circle"),
                textposition="top center",
                name="Agora",
                showlegend=True,
            )
            fig.add_trace(t)

fig.update_layout(
    xaxis_title="Hora",
    yaxis_title="Wait (min)",
    hovermode="x unified",
)

st.plotly_chart(fig, use_container_width=True)
