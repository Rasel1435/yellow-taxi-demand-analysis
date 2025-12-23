# 🚖 NYC Yellow Taxi Demand: End-to-End MLOps System
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/ML-Pipeline%20with%20ZenML-orange)](https://zenml.io/)
[![Tracking](https://img.shields.io/badge/Experiment-MLflow-yellow)](https://mlflow.org/)
[![API](https://img.shields.io/badge/API-FastAPI-green)](https://fastapi.tiangolo.com/)
[![Container](https://img.shields.io/badge/Container-Docker-blue)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

> 🚀 A fully reproducible end-to-end MLOps project built with ZenML, MLflow, and FastAPI.

This project implements a production-grade MLOps ecosystem for predicting taxi demand in New York City. It features automated ETL, Continuous Deployment, and a Live Interactive Dashboard.

---

## 🧠 Project Overview

Analyze NYC Yellow Taxi Trip Records to identify **high-demand areas (hotspots)** and build an **end-to-end ML pipeline** — from **data collection → preprocessing → model training → deployment → monitoring.**

This project demonstrates a **complete MLOps workflow** using:
## 🏗️ Technical Stac
- **Python, Pandas, scikit-learn** for preprocessing and modeling 
- **ZenML** for pipeline orchestration & experiment/metadata tracking
- **MLflow** for pipeline Experiment tracking & Model Registry
- **FastAPI** for serving  
- **Docker** for containerization  
- **Prometheus + Grafana** for monitoring
- **Modeling** Scikit-Learn (RandomForest, PCA for Dimensionality Reduction)
- **API** FastAPI (V2 Inference Protocol compliant)
- **Dashboard** Streamlit & Pydeck (3D Spatial Visualization)
- **Infrastructure** Docker & Docker Compose

---
## 🚀 Key Features
**1. Automated CI/CD for ML**
The project uses a **Continuous Deployment** pipeline that acts as a quality gate.
- **Deployment Trigger:** A model is only promoted to production if it meets strict performance thresholds (e.g., $R^2 > 0.90$ and $MAPE < 0.45$).
- **Automated Serving:** Once validated, ZenML automatically deploys the model to an MLflow prediction server.
  
**2. Feature Engineering & Scaling**
To handle the complexity of NYC taxi data, the system performs:
- **Temporal Engineering:** Extracting cyclical patterns (Hour, Day of Week, Month).
- **Lag & Window Features:** Incorporating historical trends to improve forecasting accuracy.
- **Dimensionality Reduction:** Utilizing **PCA** to reduce feature noise while maintaining 95% of data variance.
  
**3. Real-time Inference API**
The FastAPI backend is optimized for low-latency predictions.
- **Cold-Start Handling:** Implements smart imputation (Zero-filling) for real-time requests where historical lag data is missing.
- **Schema Validation:** Uses Pydantic to ensure high data integrity for incoming requests.

---

## 📂 Project Structure
```
0_Research_and_Study/ # Domain study, problem definition, model exploration
1_Data_Preprocess/ # EDA, cleaning, feature engineering, normalization
2_Model_Development/ # Model experimentation, evaluation, hyperparameter tuning
3_Data/
├── raw/ # Original TLC/NYC trip datasets
├── processed/ # Cleaned datasets after preprocessing
├── test/ # Test data for final evaluation
├── artifacts/ # Saved encoder, scaler, PCA, trained models
└── predictions/ # Model predictions storage
4_Pipelines/
├── f_pipeline/ # Main ZenML feature/training/inference pipelines
└── h_steps/ # Individual step scripts (load, clean, train, etc.)
5_API_Service/ # FastAPI app (main.py, schemas.py, router_predict.py)
6_Docker_Deployment/ # Dockerfile, docker-compose, start.sh
7_Monitoring/ # Prometheus, Grafana, logging configuration
8_Tests/ # Unit & integration tests
configs/ # YAML configuration files
notebooks/ # Optional ad-hoc notebooks
requirements.txt # Python dependencies
logs.py # Logging setup
zenmlNotes.txt # Notes for ZenML pipeline setup
README.md # Project documentation

```



---
## 🚥 How to Run (Docker)
Ensure you have Docker and Docker Compose installed.
**1. Clone the repository:**
```bash
git clone https://github.com/Rasel1435/yellow-taxi-demand-analysis.git
cd yellow-taxi-demand-analysis
```
**2. Launch the stack:**
```bash
docker-compose up --build
```
**3. Access the tools:**
- **Live Dashboard:** http://localhost:8501
- **API Documentation:** http://localhost:8000/docs
---

## 🚀 Quick Setup Guide

```bash
# Clone the repository
git clone https://github.com/Rasel1435/yellow-taxi-demand-analysis.git
cd yellow-taxi-demand-analysis

# Create required directories
mkdir -p a_Research_and_Study \
         b_Data_Preprocess \
         c_Model_Development \
         d_Data/raw d_Data/processed d_Data/test d_Data/artifacts d_Data/predictions \
         e_Pipelines/f_pipeline e_Pipelines/h_steps \
         f_API_Service \
         g_Docker_Deployment \
         h_Monitoring \
         i_Tests \
         configs \
         notebooks

# Create base files,
touch requirements.txt logs.py zenmlNotes.txt configs/config.yaml

```
---
## 🧱 Step-by-Step Workflow

1️⃣ **Research & Study** (0_Research_and_Study/)

-  Understand the NYC Yellow Taxi domain
-  Define the ML problem: Where and when is taxi demand highest?
-  Identify key features (pickup location, time, weather)
-  Explore candidate models and metrics

2️⃣ **Data Preprocessing** (1_Data_Preprocess/)

-  Perform EDA and visualization
-  Handle missing data and outliers
-  Encode categorical features and scale numerical ones
-  Feature selection/dimensionality reduction
-  Save processed datasets to /3_Data/processed/

3️⃣ **Model Development** (2_Model_Development/)

-  Train candidate models (RandomForest, XGBoost, LightGBM)
-  Evaluate using RMSE, R², etc.
-  Hyperparameter tuning
-  Save best model artifact in /3_Data/artifacts/

4️⃣ **Pipeline Orchestration** (4_Pipelines/)

-  Build ZenML pipelines for ETL, training, and inference
-  Track experiments with MLflow

5️⃣ **API Deployment** (5_API_Service/)

-  Serve predictions with FastAPI
-  Validate input using Pydantic
-  Add /predict, /health, /metrics endpoints

6️⃣ **Docker & Cloud Deployment** (6_Docker_Deployment/)

-  Containerize with Docker
-  Use Docker Compose to run API + MLflow + Postgres
-  Optionally deploy to AWS/GCP/Render

7️⃣ **Monitoring & Logging** (7_Monitoring/)

-  Integrate Prometheus + Grafana
-  Track model drift and performance metrics

8️⃣ **Testing** (8_Tests/)

-  Unit tests for preprocessing and feature engineering
-  Integration tests for API endpoints



| Layer           | Tools                           |
| --------------- | ------------------------------- |
| Data Processing | Python, Pandas, NumPy           |
| Modeling        | scikit-learn, XGBoost, LightGBM |
| Pipelines       | ZenML                           |
| Tracking        | MLflow                          |
| API             | FastAPI                         |
| Deployment      | Docker, docker-compose          |
| Monitoring      | Prometheus, Grafana             |
| Version Control | Git, GitHub                     |


---

### 9️⃣ ZenML & Project
```bash
## ZenML Stack Setup

- Pipeline orchestrated with **Airflow**
- Artifacts stored in **S3**
- Model deployment via **Docker** (local API) and optionally **HuggingFace** (cloud API)
- ZenML stack configuration included in `zenml/stack.yml` (no secrets)

```

### 📘 Next Improvements

✅ Add schema validation (Pydantic) </br>
✅ Add artifact versioning (scaler/encoder/model) </br>
✅ Integrate MLflow Model Registry </br>

```bash
    mlflow ui \
        --backend-store-uri sqlite:////media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/mlflow/mlflow.db \
        --default-artifact-root /media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/mlflow/artifacts
```

✅ Add unit + integration tests </br>
✅ Add monitoring dashboards

---
### 📊 Dataset Reference

Dataset: NYC Taxi & Limousine Commission (TLC) Trip Record Data </br>
📦 Official Source → [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---
🧑‍💻 Author

Sheikh Rasel Ahmed </br>
📍 Bangladesh </br>
💼 ML/AI Engineer </br>
📧 Contact via GitHub Issues 
