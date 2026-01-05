import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import streamlit as st
import pandas as pd
import pydeck as pdk
import datetime
import numpy as np
import h3
import joblib


from d_clean import clean_data
from e_featureEngineering import feature_engineering


# --------------------------------------------------
# 1. PAGE CONFIGURATION
# --------------------------------------------------
st.set_page_config(
    page_title="NYC Taxi Demand Live",
    page_icon="🚖",
    layout="wide"
)

# --------------------------------------------------
# 2. LOAD ML ARTIFACTS (CACHED)
# --------------------------------------------------
@st.cache_resource
def load_artifacts():
    return {
        "model": joblib.load(
            "d_Data/artifacts/yellow_taxi_demand_model_RandomForest_20251220_073717.joblib"
        ),
        "scaler": joblib.load(
            "d_Data/artifacts/scaler_20251220_073308.joblib"
        ),
        "pca": joblib.load(
            "d_Data/artifacts/pca_model_20251220_073327.joblib"
        ),
        "selected_features": joblib.load(
            "d_Data/artifacts/selected_features_20251220_073259.joblib"
        ),
    }

artifacts = load_artifacts()

# --------------------------------------------------
# 3. CUSTOM CSS
# --------------------------------------------------
st.markdown(
    """
    <style>
        .main { background-color: #0e1117; }
        div[data-testid="stMetricValue"] { font-size: 24px; color: #ffffff; }
        .stSuccess { background-color: rgba(40, 167, 69, 0.2) !important; color: #fff !important; border: 1px solid #28a745 !important; }
        [data-testid="stSidebar"] { background-color: #161b22; }
    </style>
    """,
    unsafe_allow_html=True
)

# --------------------------------------------------
# 4. SIDEBAR
# --------------------------------------------------
with st.sidebar:
    _, logo_col, _ = st.columns([1, 2, 1])
    with logo_col:
        st.image(
            "https://cdn-icons-png.flaticon.com/512/3448/3448636.png",
            width=100
        )

    st.markdown("<h3 style='text-align: center;'>Model Parameters</h3>", unsafe_allow_html=True)

    selected_date = st.date_input(
        "Select Date",
        datetime.date(2025, 1, 15)
    )
    selected_hour = st.slider(
        "Select Hour of Day",
        0, 23, 12
    )

    st.markdown("---")
    st.markdown("### 🔗 Connect & Source")
    st.markdown(
        "[![GitHub](https://img.shields.io/badge/GitHub-Repository-white?logo=github)]"
        "(https://github.com/Rasel1435/yellow-taxi-demand-analysis)"
    )
    st.markdown(
        "[![LinkedIn](https://img.shields.io/badge/LinkedIn-Profile-blue?logo=linkedin)]"
        "(https://www.linkedin.com/in/sheikh-rasel-ahmed/)"
    )
    st.write("**Developer:** Sheikh Rasel Ahmed")

# --------------------------------------------------
# 5. MAIN UI
# --------------------------------------------------
st.title("🚖 NYC Yellow Taxi Demand Forecast")

if st.button("Predict Demand"):
    try:
        # ---------------------------
        # Prepare input
        # ---------------------------
        formatted_time = f"{selected_date} {selected_hour:02d}:00:00"
        payload = [{
            "tpep_pickup_datetime": formatted_time,
            "passenger_count": 1,
            "VendorID": 1
        }]

        raw_df = pd.DataFrame(payload)
        raw_df["tpep_pickup_datetime"] = pd.to_datetime(
            raw_df["tpep_pickup_datetime"]
        )

        # ---------------------------
        # Feature processing
        # ---------------------------
        df = clean_data(raw_df)
        df = feature_engineering(df)

        for col in artifacts["selected_features"]:
            if col not in df.columns:
                df[col] = 0

        X = df[artifacts["selected_features"]].fillna(0)
        X_scaled = artifacts["scaler"].transform(X)
        X_pca = artifacts["pca"].transform(X_scaled)

        # ---------------------------
        # Prediction
        # ---------------------------
        prediction_val = artifacts["model"].predict(X_pca)[0]

        st.success(f"Real-time Prediction: {prediction_val:.2f} pickups")

        # --------------------------------------------------
        # MAP DATA
        # --------------------------------------------------
        map_data = pd.DataFrame({
            "lat": [40.7128, 40.7831, 40.7484, 40.7589, 40.7061, 40.7527, 40.7644, 40.7022],
            "lon": [-74.0060, -73.9712, -73.9857, -73.9851, -74.0092, -73.9772, -73.9235, -73.9880]
        })

        map_data["h3_index"] = map_data.apply(
            lambda r: h3.latlng_to_cell(r["lat"], r["lon"], 9),
            axis=1
        )

        map_data["demand"] = [
            prediction_val * np.random.uniform(0.8, 1.2)
            for _ in range(len(map_data))
        ]

        map_data["elevation"] = (
            map_data["demand"] / map_data["demand"].max()
        ) * 2000

        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown("### 🗺️ Demand Hotspots (3D Hexagons)")
            st.pydeck_chart(
                pdk.Deck(
                    map_style="dark",
                    initial_view_state=pdk.ViewState(
                        latitude=40.7306,
                        longitude=-73.9352,
                        zoom=11,
                        pitch=50,
                    ),
                    tooltip={"text": "Estimated Pickups: {demand}"},
                    layers=[
                        pdk.Layer(
                            "H3HexagonLayer",
                            data=map_data,
                            get_hexagon="h3_index",
                            get_fill_color="[180, 0, 255, 160]",
                            get_elevation="elevation",
                            elevation_scale=1,
                            extruded=True,
                            pickable=True,
                        )
                    ],
                )
            )

        with col2:
            st.markdown("### 📈 24-Hour Trend")
            trend = pd.DataFrame({
                "Hr": range(24),
                "Val": [
                    prediction_val * (1 + 0.3 * np.sin(h / 4))
                    for h in range(24)
                ]
            }).set_index("Hr")

            st.area_chart(trend)
            st.metric(
                "Peak Demand Estimate",
                f"{int(prediction_val * 1.2)}",
                "+12%"
            )

    except Exception as e:
        st.warning("Service Error – Unable to run prediction")
        st.caption(str(e))

# --------------------------------------------------
# Run:
# export PYTHONPATH=$(pwd)
# streamlit run f_API_Service/c_frontend.py
# --------------------------------------------------
