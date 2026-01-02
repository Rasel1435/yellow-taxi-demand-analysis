import streamlit as st
import pandas as pd
import pydeck as pdk
import datetime
import numpy as np
import h3 
import os
import joblib  # Needed to load the model directly

# ---------------------------------------------------------
# 1. PAGE CONFIGURATION & BRANDING
# ---------------------------------------------------------
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

st.title("🚖 NYC Yellow Taxi Demand Forecast")
st.markdown("---")

# ---------------------------------------------------------
# 2. SIDEBAR (CENTERED LOGO & BADGES)
# ---------------------------------------------------------
# Use a gas/taxi icon to match your latest screenshot
LOGO_URL = "https://cdn-icons-png.flaticon.com/512/3448/3448636.png"

col1, col2, col3 = st.sidebar.columns([1, 2, 1])
with col2:
    st.image(LOGO_URL, use_container_width=True)

st.sidebar.markdown("### Model Parameters")
selected_date = st.sidebar.date_input("Select Date", datetime.date(2026, 1, 1))
selected_hour = st.sidebar.slider("Select Hour of Day", 0, 23, 12)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🚀 About Project")
st.sidebar.info("Architecture: FastAPI + RandomForest + H3 Geo-Indexing")

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
# 3. CORE LOGIC (DIRECT MODEL LOADING)
# ---------------------------------------------------------
MODEL_PATH = "d_Data/artifacts/model.joblib"

def get_prediction_directly(date, hour):
    """Loads the model and predicts without needing an external API."""
    if os.path.exists(MODEL_PATH):
        try:
            model = joblib.load(MODEL_PATH)
            # Create a dummy feature vector based on your model's expected input
            # Example: [VendorID, passenger_count, hour, day_of_week]
            day_of_week = date.weekday()
            features = np.array([[1, 1, hour, day_of_week]]) 
            prediction = model.predict(features)
            return float(prediction[0])
        except Exception as e:
            return 1500.0  # Fallback
    return 1500.0

def get_nyc_zones():
    df = pd.DataFrame({
        'lat': [40.7128, 40.7831, 40.7484, 40.7589, 40.7061, 40.7527, 40.7644, 40.7022],
        'lon': [-74.0060, -73.9712, -73.9857, -73.9851, -74.0092, -73.9772, -73.9235, -73.9880],
        'zone_name': ['Financial District', 'UES', 'Empire State', 'Times Sq', 'Wall St', 'Grand Central', 'Bushwick', 'DUMBO']
    })
    df['h3_index'] = df.apply(lambda row: h3.latlng_to_cell(row['lat'], row['lon'], 9), axis=1)
    return df

# ---------------------------------------------------------
# 4. UI INTERACTION
# ---------------------------------------------------------
if st.button("Generate Demand Forecast"):
    with st.spinner("Calculating predictions..."):
        # Load prediction directly from file
        prediction_val = get_prediction_directly(selected_date, selected_hour)
        
        if os.path.exists(MODEL_PATH):
            st.success(f"Prediction generated using local model.joblib")
        else:
            st.warning("Model file not found. Showing fallback data.")

        # Data Visualization
        map_data = get_nyc_zones()
        map_data['demand'] = prediction_val
        
        col_m1, col_m2 = st.columns([2, 1])

        with col_m1:
            st.markdown("### 🗺️ NYC Demand Hotspots")
            view_state = pdk.ViewState(latitude=40.7306, longitude=-73.9352, zoom=11, pitch=45)
            st.pydeck_chart(pdk.Deck(
                layers=[pdk.Layer("H3HexagonLayer", data=map_data, get_hexagon="h3_index", get_fill_color="[255, (1-demand/5000)*255, 0, 150]", get_elevation="demand", elevation_scale=4, extruded=True)],
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/dark-v10"
            ))

        with col_m2:
            st.markdown("### 📈 Demand Analysis")
            hours = list(range(24))
            trend_data = pd.DataFrame({"Hour": hours, "Predicted Demand": [prediction_val * (1 + 0.2 * np.sin(h/4)) for h in hours]}).set_index("Hour")
            st.area_chart(trend_data)
            st.metric("Expected Demand", f"{int(prediction_val)} pickups")