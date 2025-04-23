import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pydeck as pdk
import math
from io import BytesIO
from azure.storage.blob import BlobServiceClient


AZURE_CS = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=iesstsabbadbaa;"
    "AccountKey=ZT6z+TYSxF0Xdm0vOCRbIpWoBss2BxOU0EcP2UDceddHX7Kyi8gyJvjyWG5THNp2HOprCHmblb2f+AStp8mAGw==;"
    "EndpointSuffix=core.windows.net"
)
CONTAINER = "streamed-data-group8"

@st.cache_data
def load_parquet_from_blob(prefix: str) -> pd.DataFrame:
    svc = BlobServiceClient.from_connection_string(AZURE_CS)
    ctr = svc.get_container_client(CONTAINER)
    blobs = [
        b.name for b in ctr.list_blobs(name_starts_with=prefix)
        if b.name.lower().endswith(".parquet")
    ]
    dfs = []
    for name in sorted(blobs):
        bio = BytesIO()
        svc.get_blob_client(CONTAINER, name) \
           .download_blob() \
           .readinto(bio)
        bio.seek(0)
        dfs.append(pd.read_parquet(bio))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


ride_df   = load_parquet_from_blob("ride-requests-output/")
driver_df = load_parquet_from_blob("drivers-output/")


ride_df["timestamp"] = pd.to_datetime(ride_df["timestamp"])
ride_df["hour"]      = ride_df["timestamp"].dt.hour


st.sidebar.title("⚙️ Filters")
hr_min, hr_max = st.sidebar.slider("Hour range", 0, 23, (0, 23))
vehicle_types  = st.sidebar.multiselect(
    "Vehicle Types",
    options=sorted(ride_df["vehicle_type"].unique()),
    default=sorted(ride_df["vehicle_type"].unique())
)
top_n = st.sidebar.number_input("Top N Drivers", min_value=5, max_value=30, value=10)

# Filters:
ride_df = ride_df[
    (ride_df.hour >= hr_min) &
    (ride_df.hour <= hr_max) &
    (ride_df.vehicle_type.isin(vehicle_types))
]

# 
st.title("🚖 Real-Time Ride-Hailing Dashboard 🚖")

# For the KPI's:
if not ride_df.empty:
    total_rides = len(ride_df)
    avg_fare    = ride_df["fare_euros"].mean()
    avg_eta     = ride_df["pickup_eta_min"].mean()
    peak_hour   = int(ride_df.groupby("hour").size().idxmax())
else:
    total_rides = avg_fare = avg_eta = 0
    peak_hour = None

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Rides",      f"{total_rides}")
k2.metric("Avg Fare (€)",     f"{avg_fare:.2f}")
k3.metric("Avg ETA (min)",    f"{avg_eta:.1f}")
k4.metric("Peak Hour",        f"{peak_hour}:00" if peak_hour is not None else "-")

# 🟢 BASIC USE CASE  
st.header("🟢 Hourly Ride Metrics")
agg = (
    ride_df
    .groupby("hour")
    .agg(
      Total_Rides         = ("request_id",               "count"),
      Avg_ETA_to_Pickup   = ("pickup_eta_min",           "mean"),
      Avg_Fare            = ("fare_euros",               "mean"),
      Avg_Traffic_Congest = ("traffic_delay_multiplier", "mean")
    )
    .reset_index()
)
st.plotly_chart(px.bar(agg, x="hour", y="Total_Rides",         title="Rides per Hour"), use_container_width=True)
st.plotly_chart(px.line(agg, x="hour", y="Avg_ETA_to_Pickup",    title="Avg ETA to Pickup (min)"), use_container_width=True)
st.plotly_chart(px.line(agg, x="hour", y="Avg_Fare",             title="Avg Fare (€)"), use_container_width=True)
st.plotly_chart(px.line(agg, x="hour", y="Avg_Traffic_Congest",  title="Avg Traffic Congestion"), use_container_width=True)

# 🟡 INTERMEDIATE USE CASE  
st.header("🟡 Demand-supply & satisfaction")
supply = driver_df.groupby("shift_start").size().reset_index(name="Active_Drivers")
demand = ride_df .groupby("hour").size().reset_index(name="Requested_Rides")
ds = pd.merge(demand, supply, left_on="hour", right_on="shift_start", how="left").fillna(0)
st.plotly_chart(px.bar(ds, x="hour", y=["Requested_Rides","Active_Drivers"],
                       barmode="group", title="Demand vs Supply by Hour"), use_container_width=True)
st.plotly_chart(px.line(
    ride_df.groupby("hour")["driver_rating_given"]
           .mean().reset_index(name="Avg_Rating"),
    x="hour", y="Avg_Rating", title="Passenger Satisfaction (Rating)"
), use_container_width=True)

# 🟣 ADVANCED DRIVER UTILIZATION & WORKLOAD  
st.header("🟣 Driver utilization & workload")
drv = (
    ride_df
    .groupby("driver_name")
    .agg(
      rides_served     = ("request_id",              "count"),
      avg_distance_km  = ("distance_km",             "mean"),
      avg_duration_min = ("estimated_ride_time_min", "mean"),
      avg_rating       = ("driver_rating_given",     "mean")
    )
    .reset_index()
    .sort_values("rides_served", ascending=False)
    .head(int(top_n))
)
st.subheader(f"🏆 Top {top_n} Busiest Drivers")
st.bar_chart(drv.set_index("driver_name")["rides_served"])

drv_m = drv.melt(
    id_vars="driver_name",
    value_vars=["avg_distance_km","avg_duration_min"],
    var_name="Metric", value_name="Value"
)
st.plotly_chart(px.bar(
    drv_m, x="driver_name", y="Value", color="Metric", barmode="group",
    title="Avg Distance (km) & Duration (min)"
), use_container_width=True)

# Driver's Performance:
st.subheader("Driver's performance")
N = len(drv)
cols = min(N, 5)
rows = math.ceil(N/cols)
fig_g = make_subplots(
    rows=rows, cols=cols,
    specs=[[{"type":"domain"}]*cols for _ in range(rows)],
    subplot_titles=list(drv["driver_name"])
)
for i, r in enumerate(drv.itertuples()):
    rr = (i // cols) + 1
    cc = (i % cols) + 1
    fig_g.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=round(r.avg_rating,2),
            number={"font":{"size":24}},
            gauge={
                "axis":{"range":[0,5]},
                "bar":{"color":"darkblue"},
                "steps":[
                    {"range":[0,2], "color":"#ffcccc"},
                    {"range":[2,4], "color":"#ffe699"},
                    {"range":[4,5], "color":"#ccffcc"}
                ]
            }
        ),
        row=rr, col=cc
    )

for ann in fig_g.layout.annotations:
    ann.font = dict(size=10)

fig_g.update_layout(
    height=150*rows,
    margin={"t":40,"b":0},
    title_text="Driver Avg. Rating",
    showlegend=False
)
st.plotly_chart(fig_g, use_container_width=True)

# COVERAGE HEATMAP  
st.header("Driver coverage heatmap")
mid = (
    (driver_df.latitude.mean(), driver_df.longitude.mean())
    if not driver_df.empty else (40.42, -3.70)
)
heatmap = pdk.Deck(
    initial_view_state=pdk.ViewState(latitude=mid[0], longitude=mid[1], zoom=11, pitch=50),
    layers=[pdk.Layer(
        "HeatmapLayer", data=driver_df,
        get_position=["longitude","latitude"], radiusPixels=60
    )],
)
st.pydeck_chart(heatmap, use_container_width=True)
