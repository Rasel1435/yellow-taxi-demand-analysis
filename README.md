# yellow-taxi-demand-analysis
End-to-end ML pipeline for analyzing Yellow Taxi demand hotspots in NYC

# 🚖 Yellow Taxi Demand Analysis
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/ML-Pipeline%20with%20ZenML-orange)](https://zenml.io/)
[![Tracking](https://img.shields.io/badge/Experiment-MLflow-yellow)](https://mlflow.org/)
[![API](https://img.shields.io/badge/API-FastAPI-green)](https://fastapi.tiangolo.com/)
[![Container](https://img.shields.io/badge/Container-Docker-blue)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

> 🚀 A fully reproducible end-to-end MLOps project built with ZenML, MLflow, and FastAPI.

End-to-end ML pipeline for analyzing Yellow Taxi demand hotspots in NYC.

---

## 🧠 Project Overview

Analyze NYC Yellow Taxi Trip Records to identify **high-demand areas (hotspots)** and build an **end-to-end ML pipeline** — from **data collection → preprocessing → model training → deployment → monitoring.**

This project demonstrates a **complete MLOps workflow** using:
- **Python, Pandas, scikit-learn** for preprocessing and modeling  
- **ZenML + MLflow** for pipeline orchestration and experiment tracking  
- **FastAPI** for serving  
- **Docker** for containerization  
- **Prometheus + Grafana** for monitoring  

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

## 🚀 Quick Setup Guide

```bash
# Clone the repository
git clone https://github.com/Rasel1435/yellow-taxi-demand-analysis.git
cd yellow-taxi-demand-analysis

# Create required directories
mkdir -p 0_Research_and_Study \
         1_Data_Preprocess \
         2_Model_Development \
         3_Data/raw 3_Data/processed 3_Data/test 3_Data/artifacts 3_Data/predictions \
         4_Pipelines/f_pipeline 4_Pipelines/h_steps \
         5_API_Service \
         6_Docker_Deployment \
         7_Monitoring \
         8_Tests \
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
### 📘 Next Improvements

✅ Add schema validation (Pydantic) </br>
✅ Add artifact versioning (scaler/encoder/model) </br>
✅ Integrate MLflow Model Registry </br>
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
