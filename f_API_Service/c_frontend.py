import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
import datetime
import numpy as np
import h3 
import os

# ---------------------------------------------------------
# STREAMLIT APP: NYC TAXI DEMAND VISUALIZATION
# ---------------------------------------------------------
st.set_page_config(page_title="NYC Taxi Demand Live", layout="wide")
st.title("🚖 NYC Yellow Taxi Demand Forecast")

# 1. Sidebar Configuration
st.sidebar.header("Model Parameters")
selected_date = st.sidebar.date_input("Select Date", datetime.date(2025, 1, 1))
selected_hour = st.sidebar.slider("Select Hour of Day", 0, 23, 12)

# 2. Data Preparation Functions
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
    normalized = min(1.0, demand / 5000)
    r = int(138 + (117 * normalized))
    g = int(43 * (1 - normalized))
    b = 255 
    return [r, g, b, 255]

# 3. Main Prediction Logic
if st.button("Predict Demand"):
    with st.spinner("Connecting to API and fetching predictions..."):
        # FETCH ENVIRONMENT VARIABLE
        API_URL = os.getenv("API_URL", "http://api:8000/predict")
        
        try:
            formatted_time = f"{selected_date} {selected_hour:02d}:00:00"
            payload = [{"tpep_pickup_datetime": formatted_time, "passenger_count": 1, "VendorID": 1}]
            
            # --- THE KEY FIX: INCREASE TIMEOUT TO 30s ---
            response = requests.post(API_URL, json=payload, timeout=30)
            
            if response.status_code == 200:
                prediction_val = response.json()[0]['predicted_taxi_demand']
                st.success(f"Real-time Prediction: {prediction_val:.2f} pickups")
            else:
                st.error(f"API Error: {response.status_code}")
                prediction_val = 1500.0 # Fallback
        
        except requests.exceptions.Timeout:
            st.error("⌛ Request Timed Out. The API is taking too long to process the model.")
            prediction_val = 1500.0
        except Exception as e:
            st.warning(f"Connection to API ({API_URL}) failed.")
            with st.expander("Technical details"):
                st.write(str(e))
            prediction_val = 1500.0 

        # --- DATA PROCESSING & DASHBOARD ---
        map_data = get_nyc_zones()
        map_data['demand'] = prediction_val
        map_data['color'] = map_data['demand'].apply(get_demand_color)
        
        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown("### 🗺️ Demand Hotspots (3D Hexagons)")
            view_state = pdk.ViewState(latitude=40.7306, longitude=-73.9352, zoom=11, pitch=50)
            
            hexagon_layer = pdk.Layer(
                "H3HexagonLayer",
                data=map_data,
                get_hexagon="h3_index",
                get_fill_color="color",
                get_elevation="demand",
                elevation_scale=4,
                extruded=True,
                pickable=True,
            )
            
            st.pydeck_chart(pdk.Deck(
                layers=[hexagon_layer], 
                initial_view_state=view_state,
                height=600,
                tooltip={"text": "{zone_name}\nDemand: {demand}"},
                map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
            ))

        with col2:
            st.markdown("### 📈 24-Hour Trend")
            hours = list(range(24))
            trend_data = pd.DataFrame({
                "Hour": hours,
                "Demand": [prediction_val * (1 + 0.3 * np.sin(h/4)) for h in hours]
            }).set_index("Hour")
            
            st.line_chart(trend_data, height=400)
            st.metric(label="Peak Demand Estimate", value=f"{int(prediction_val * 1.3)}", delta="+12%")