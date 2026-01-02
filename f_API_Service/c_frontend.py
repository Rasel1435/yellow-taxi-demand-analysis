import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
import datetime
import numpy as np
import h3 
import os
import joblib

# ---------------------------------------------------------
# 1. PATHS & CONFIGURATION
# ---------------------------------------------------------
# Your specific model filename from your artifacts folder
MODEL_PATH = "d_Data/artifacts/yellow_taxi_demand_model_RandomForest_20251220_073717.joblib"

st.set_page_config(
    page_title="NYC Taxi Demand Live", 
    page_icon="🚖",
    layout="wide"
)

# Professional CSS for centering and modern styling
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        border: 1px solid #eee;
    }
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
        text-align: center;
        align-items: center;
    }
    </style>
    """, unsafe_allow_html=True)

# ---------------------------------------------------------
# 2. SIDEBAR CONFIGURATION (CENTER ALIGNED)
# ---------------------------------------------------------
LOGO_URL = "https://cdn-icons-png.flaticon.com/512/3448/3448636.png"

col1, col2, col3 = st.sidebar.columns([1, 2, 1])
with col2:
    st.image(LOGO_URL, use_container_width=True)

st.sidebar.markdown("### Model Parameters")
selected_date = st.sidebar.date_input("Select Date", datetime.date(2026, 1, 1))
selected_hour = st.sidebar.slider("Select Hour of Day", 0, 23, 12)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🚀 About Project")
st.sidebar.info(
    """
    **Architecture:**
    - **Backend:** FastAPI (Dockerized)
    - **Model:** RandomForest + PCA
    - **Spatial:** H3 Geo-Indexing
    - **Pipeline:** ZenML / MLOps
    """
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 👨‍💻 Developer")
st.sidebar.write("**Sheikh Rasel Ahmed**")

st.sidebar.markdown(f"""
<div style="display: flex; flex-direction: column; align-items: center; gap: 12px; width: 100%;">
    <a href="https://github.com/Rasel1435/yellow-taxi-demand-analysis" target="_blank" style="width: 80%;">
        <img src="https://img.shields.io/badge/GitHub-Repository-white?style=for-the-badge&logo=github&logoColor=black" style="width: 100%;">
    </a>
    <a href="https://www.linkedin.com/in/shekhnirob1/" target="_blank" style="width: 80%;">
        <img src="https://img.shields.io/badge/LinkedIn-Profile-blue?style=for-the-badge&logo=linkedin" style="width: 100%;">
    </a>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# 3. HELPER FUNCTIONS
# ---------------------------------------------------------
def get_nyc_zones():
    df = pd.DataFrame({
        'lat': [40.7128, 40.7831, 40.7484, 40.7589, 40.7061, 40.7527, 40.7644, 40.7022],
        'lon': [-74.0060, -73.9712, -73.9857, -73.9851, -74.0092, -73.9772, -73.9235, -73.9880],
        'zone_name': ['Financial District', 'Upper East Side', 'Empire State', 'Times Square', 
                     'Wall St', 'Grand Central', 'Bushwick', 'DUMBO']
    })
    df['h3_index'] = df.apply(lambda row: h3.latlng_to_cell(row['lat'], row['lon'], 9), axis=1)
    return df

def get_demand_color(demand):
    # Purple/Magenta scale matching your local screenshot
    return [180, 0, 255, 160]

# ---------------------------------------------------------
# 4. MAIN PREDICTION LOGIC (HYBRID CLOUD/LOCAL)
# ---------------------------------------------------------
st.title("🚖 NYC Yellow Taxi Demand Forecast")
st.markdown("---")

if st.button("Generate Demand Forecast"):
    prediction_val = None
    
    # STEP A: Try Direct Model Loading (Perfect for Streamlit Cloud)
    if os.path.exists(MODEL_PATH):
        try:
            with st.spinner("Processing with local model file..."):
                model = joblib.load(MODEL_PATH)
                # Ensure input matches your model's expected shape
                day_of_week = selected_date.weekday()
                # Assuming your features were: [VendorID, passengers, hour, day_of_week]
                # Adjust features based on your exact training data columns
                features = np.array([[1, 1, selected_hour, day_of_week]])
                prediction_val = float(model.predict(features)[0])
                st.success(f"Prediction generated: {prediction_val:.2f} pickups")
        except Exception as e:
            st.error(f"Model Load Error: {e}")

    # STEP B: Try API Connection (Backup for local Docker)
    if prediction_val is None:
        try:
            with st.spinner("Connecting to API..."):
                API_URL = os.getenv("API_URL", "http://api:8000/predict")
                formatted_time = f"{selected_date} {selected_hour:02d}:00:00"
                payload = [{"tpep_pickup_datetime": formatted_time, "passenger_count": 1, "VendorID": 1}]
                response = requests.post(API_URL, json=payload, timeout=5)
                if response.status_code == 200:
                    prediction_val = response.json()[0]['predicted_taxi_demand']
                    st.success(f"🚀 API Prediction: {prediction_val:.2f} pickups")
        except:
            pass

    # STEP C: Emergency Fallback
    if prediction_val is None:
        st.warning("API Offline - Using Fallback Demo Data")
        prediction_val = 1500.0

    # ---------------------------------------------------------
    # 5. VISUALIZATION SECTION
    # ---------------------------------------------------------
    map_data = get_nyc_zones()
    map_data['demand'] = prediction_val
    map_data['color'] = map_data['demand'].apply(get_demand_color)
    
    col_m1, col_m2 = st.columns([2, 1])

    with col_m1:
        st.markdown("### 🗺️ NYC Demand Hotspots (3D Hexagons)")
        view_state = pdk.ViewState(latitude=40.7306, longitude=-73.9352, zoom=11, pitch=45)
        
        hexagon_layer = pdk.Layer(
            "H3HexagonLayer",
            data=map_data,
            get_hexagon="h3_index",
            get_fill_color="color",
            get_elevation="demand",
            elevation_scale=5, # Higher scale for more dramatic purple bars
            extruded=True,
            pickable=True,
        )
        
        st.pydeck_chart(pdk.Deck(
            layers=[hexagon_layer], 
            initial_view_state=view_state,
            height=600,
            tooltip={"text": "Predicted Pickups: {demand}"},
            map_style="mapbox://styles/mapbox/dark-v10"
        ))

    with col_m2:
        st.markdown("### 📈 Demand Analysis")
        hours = list(range(24))
        trend_data = pd.DataFrame({
            "Hour": hours,
            "Predicted Demand": [prediction_val * (1 + 0.3 * np.sin(h/4)) for h in hours]
        }).set_index("Hour")
        
        st.area_chart(trend_data, height=350)
        
        st.metric(label="Selected Hour Demand", value=f"{int(prediction_val)}")
        st.metric(label="Expected Daily Peak", value=f"{int(prediction_val * 1.3)}", delta="+12% vs Avg")

st.markdown("---")
st.caption("Data: NYC TLC Trip Records | Model: RandomForestRegressor")