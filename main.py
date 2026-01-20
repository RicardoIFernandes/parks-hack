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
    try:
        h, m = hhmm.split(":")
        h = int(h)
        m = int(m)
        m_bucket = 0 if m < 30 else 30
        return f"{h:02d}:{m_bucket:02d}"
    except Exception:
        return hhmm[:5]

def classify_wait(wait_now, avg, p25, p75, tol=2):
    if pd.isna(wait_now) or pd.isna(avg) or pd.isna(p25) or pd.isna(p75):
        return "Sem dados", "⚪"
    if wait_now > p75:
        return "Muito ruim", "🔴🔴"
    elif wait_now > avg:
        return "Ruim", "🔴"
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
# LIVE API (por park_id)
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
    park_name = get_park_name_by_id(park_id)
    payload = fetch_live_queues_for_park(park_id)

    ts_utc_iso = datetime.now(timezone.utc).isoformat()
    ts_florida_dt = (
        pd.to_datetime(ts_utc_iso, utc=True)
        .tz_convert("Etc/GMT+5")   # UTC-5 fixo (igual CSV)
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
    park_selected = st.selectbox("Park", options=parks, index=min(1, len(parks)-1))

    rides = get_rides_for_park_from_df(df_all, park_selected)
    if not rides:
        st.warning("Nenhum ride encontrado para este park no CSV.")
        st.stop()

    ride_filter_text = st.text_input("Filtrar rides (opcional)", value="").strip().lower()
    ride_options = [r for r in rides if ride_filter_text in r.lower()] if ride_filter_text else rides
    if not ride_options:
        ride_options = rides

    ride_selected = st.selectbox("Ride", options=ride_options, index=0)

    # No celular, 500 pontos fica pesado. 150 costuma ficar bem.
    limit_rows = st.slider("Pontos no gráfico", min_value=50, max_value=400, value=150, step=25)

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
df_class = None

park_id_selected = get_park_id_from_csv(df_all, park_selected)
if park_id_selected is not None:
    live = build_live_dfs_for_park(park_id_selected)
    df_live_join = live["df_join"]
    df_live_ride_now = df_live_join[df_live_join["ride_name"] == ride_selected].copy()

    baseline = (
        df_all[df_all["park_name"] == park_selected]
        .groupby("ride_name", as_index=False)
        .agg(
            avg_wait=("avg_wait", "mean"),
            p25_wait=("p25_wait", "mean"),
            p75_wait=("p75_wait", "mean"),
        )
    )

    df_class = df_live_join.merge(baseline, on="ride_name", how="left")
    df_class["classificacao"] = df_class.apply(
        lambda r: classify_wait(r.get("wait_min"), r.get("avg_wait"), r.get("p25_wait"), r.get("p75_wait"))[0],
        axis=1
    )
    df_class["icon"] = df_class.apply(
        lambda r: classify_wait(r.get("wait_min"), r.get("avg_wait"), r.get("p25_wait"), r.get("p75_wait"))[1],
        axis=1
    )

# -----------------------------
# Topo mobile: métricas + lista ruim
# -----------------------------
with st.expander("📡 Agora (ao vivo) + classificação", expanded=True):
    if park_id_selected is None:
        st.warning("Não encontrei park_id no CSV para esse park_name.")
    else:
        # Em celular, columns 1/1 fica apertado; empilha se quiser:
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
                st.caption(f"Hora FL (UTC-5): {hhmm} | Bucket: {bucket}")

                if df_class is not None and not df_class.empty:
                    rr = df_class[df_class["ride_name"] == ride_selected]
                    if not rr.empty:
                        rr0 = rr.iloc[0]
                        label, icon = classify_wait(rr0.get("wait_min"), rr0.get("avg_wait"), rr0.get("p25_wait"), rr0.get("p75_wait"))
                        st.markdown(f"### {icon} {label}")

        with c2:
            st.write("Filas Agora")
            if df_class is None or df_class.empty:
                st.info("Classificação indisponível.")
            else:
                df_show = (
                    df_class[["icon", "ride_name", "wait_min", "classificacao"]]
                    .dropna(subset=["wait_min"])
                    .sort_values("wait_min", ascending=False)
                    .set_index("wait_min")
                    
                )
                st.dataframe(df_show, use_container_width=True, height=280)

# -----------------------------
# Gráfico mobile-friendly
# - sem facet (1 gráfico só)
# - eixo X contínuo (datetime) para zoom/pan natural
# - dragmode pan, scrollZoom, doubleClick reset
# -----------------------------
# Converte "hora_str" para um datetime fictício (mesmo dia), só para o Plotly tratar como contínuo
base_date = pd.Timestamp("2000-01-01")
df["x_dt"] = base_date + pd.to_timedelta(df["hora_str"] + ":00")

# Banda P25–P75 via scatter (fill=tonexty) funciona melhor pro mobile
band = df[["x_dt", "p25_wait", "p75_wait"]].copy()

fig = px.line(
    df,
    x="x_dt",
    y=["avg_wait", "min_wait", "max_wait"],
    markers=True,
    title=f"{park_selected} — {ride_selected}",
)

# Ajusta labels da legenda
rename_map = {"avg_wait": "Média", "min_wait": "Mínimo", "max_wait": "Máximo"}
for tr in fig.data:
    if tr.name in rename_map:
        tr.name = rename_map[tr.name]

# Adiciona banda P25–P75 como duas linhas invisíveis preenchidas
fig_band_top = px.scatter(band, x="x_dt", y="p75_wait")
fig_band_bot = px.scatter(band, x="x_dt", y="p25_wait")

# top (invisível)
top_trace = fig_band_top.data[0]
top_trace.update(mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip")
fig.add_trace(top_trace)

# bottom (invisível) com fill
bot_trace = fig_band_bot.data[0]
bot_trace.update(mode="lines", line=dict(width=0), fill="tonexty", name="P25–P75")
fig.add_trace(bot_trace)

# PONTO "AGORA" alinhado ao bucket (mas agora em datetime contínuo)
if not df_live_ride_now.empty:
    r0 = df_live_ride_now.iloc[0]
    wait_now = r0.get("wait_min")
    bucket = r0.get("hora_florida_bucket")

    if pd.notna(wait_now) and isinstance(bucket, str):
        bucket_dt = base_date + pd.to_timedelta(bucket + ":00")

        fig_now = px.scatter(
            pd.DataFrame([{"x_dt": bucket_dt, "wait": float(wait_now), "label": "AGORA"}]),
            x="x_dt",
            y="wait",
            text="label",
        )
        now_trace = fig_now.data[0]
        now_trace.update(
            marker=dict(size=16, color="red", symbol="circle"),
            textposition="top center",
            name="Agora",
            showlegend=True,
        )
        fig.add_trace(now_trace)

# Layout/zoom mobile-friendly
fig.update_layout(
    height=520,
    margin=dict(l=10, r=10, t=60, b=10),
    hovermode="x unified",
    xaxis_title="Hora (FL)",
    yaxis_title="Wait (min)",
    # ajuda no celular: botões maiores/mais úteis
    legend_title_text="",
)

# Eixo X como horas bonitas
fig.update_xaxes(
    tickformat="%H:%M",
    dtick=30 * 60 * 1000,  # 30 minutos em ms
)

# Config do Plotly: pan/zoom fácil no mobile
config = dict(
    responsive=True,
    displayModeBar=True,       # mostra barra (no mobile dá zoom/reset)
    scrollZoom=True,           # zoom com scroll / trackpad
    doubleClick="reset",       # duplo clique reseta
)

# dica rápida
st.caption("Dica mobile: use **pinch-to-zoom** no gráfico e arraste para navegar.")

st.plotly_chart(fig, use_container_width=True, config=config)
