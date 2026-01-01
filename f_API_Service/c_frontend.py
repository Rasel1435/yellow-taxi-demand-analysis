import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
import datetime
import numpy as np

st.set_page_config(page_title="NYC Taxi Demand Live", layout="wide")

st.title("🚖 NYC Yellow Taxi Demand Forecast")

# 1. Sidebar Configuration
st.sidebar.header("Model Parameters")
selected_date = st.sidebar.date_input("Select Date", datetime.date(2025, 1, 1))
selected_hour = st.sidebar.slider("Select Hour of Day", 0, 23, 12)

# 2. Mock Data for Mapping (NYC Zones)
def get_nyc_zones():
    return pd.DataFrame({
        'lat': [40.7128, 40.7831, 40.7484, 40.7589, 40.7061, 40.7527, 40.7644, 40.7022],
        'lon': [-74.0060, -73.9712, -73.9857, -73.9851, -74.0092, -73.9772, -73.9235, -73.9880],
        'zone_name': ['Financial District', 'Upper East Side', 'Empire State', 'Times Square', 
                     'Wall St', 'Grand Central', 'Bushwick', 'DUMBO']
    })

def get_demand_color(demand):
    # Scale: 0 to 5000
    normalized = min(1.0, demand / 5000)
    
    # Transition from Electric Violet to Neon Magenta
    # Low Demand (Violet): [138, 43, 226]
    # High Demand (Neon Magenta): [255, 0, 255]
    
    r = int(138 + (117 * normalized))
    g = int(43 * (1 - normalized))
    b = int(226 + (29 * normalized))
    
    # Use 255 alpha for maximum "pop" so it doesn't blend with the map
    return [r, g, b, 255]


# 3. Main Logic
if st.button("Predict Demand"):
    with st.spinner("Fetching predictions..."):
        try:
            API_URL = "http://localhost:8000/predict" 
            formatted_time = f"{selected_date} {selected_hour:02d}:00:00"
            
            payload = [{"tpep_pickup_datetime": formatted_time, "passenger_count": 1, "VendorID": 1}]
            response = requests.post(API_URL, json=payload)
            response.raise_for_status()
            prediction_val = response.json()[0]['predicted_taxi_demand']
            
            # Map Processing
            map_data = get_nyc_zones()
            map_data['demand'] = prediction_val
            map_data['color'] = map_data['demand'].apply(get_demand_color)
            
            st.success(f"Current Prediction: {prediction_val:.2f} pickups")

            # --- MAP SECTION ---
            # Use CartoDb style to ensure the map shows without a Mapbox Token
            view_state = pdk.ViewState(latitude=40.7306, longitude=-73.9352, zoom=10, pitch=45)

            base_layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_data,
                get_position=["lon", "lat"],
                get_color="color",
                get_radius=500,
                pickable=True,
            )

            tower_layer = pdk.Layer(
                "ColumnLayer",
                data=map_data,
                get_position=["lon", "lat"],
                get_elevation="demand",
                elevation_scale=0.5, # Lowered scale since demand is in thousands
                radius=300,
                get_fill_color="color",
                pickable=True,
            )

            st.pydeck_chart(pdk.Deck(
                layers=[base_layer, tower_layer], 
                initial_view_state=view_state,
                tooltip={"text": "{zone_name}\nDemand: {demand}"},
                map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
            ))

            # --- METRICS SECTION ---
            st.markdown("### 24-Hour Trend Forecast")
            
            # Simulate trend based on your model's current behavior
            # In a real app, you'd loop 24 API calls here
            hours = list(range(24))
            trend_data = pd.DataFrame({
                "Hour": hours,
                "Predicted Demand": [prediction_val * (1 + 0.2 * np.sin(h/4)) for h in hours]
            }).set_index("Hour")
            
            st.line_chart(trend_data)
            
        except Exception as e:
            st.error(f"Error: {e}")


# streamlit run f_API_Service/c_frontend.py