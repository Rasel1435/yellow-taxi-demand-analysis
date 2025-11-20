## ⚡ Running Pipelines & ZenML Dashboard

All ETL, feature engineering, and feature selection steps are orchestrated with **ZenML**. You can run the full pipeline and monitor each step via the ZenML dashboard.

### 🛠 0️⃣ Initialize ZenML (Required)

Before running any pipeline, initialize ZenML in your project root:
```bash
zenml init
```
This command creates the .zen/ directory and sets up your local ZenML stack and workspace.

### 📌 Important:
Do **NOT** commit the **.zen/** folder — add it to **.gitignore**:
```bash
.zen/
```
### 🔍 0.1 Verify ZenML Configuration
Shows active project, stack, server, and versions:
```bash
zenml status
```
List available stacks.
```bash
zenml stack list
```
List available projects
```bash
zenml project list
```
Describe active project
```bash
zenml project describe
```
Lists available workspaces.


### 1️⃣ Install ZenML and server dependencies

Make sure ZenML and the server extras are installed:

```bash
pip install zenml==0.91.2
pip install "zenml[server]==0.91.2"
```
### 2️⃣ Run the full pipeline

```bash
python 4_Pipelines/run_pipeline.py
```

ZenML will execute the steps:
1. **Data Ingestion** (a_ingest.py)
2. **Data Cleaning** (b_clean.py)
3. **Feature Engineering** (c_featureEngineering.py)
4. **Feature Selection** (d_featuresSelection.py)
   
Each step is logged automatically, and the output artifacts (dataframes, feature lists) are tracked.

### 3️⃣ Start ZenML dashboard

To visualize the pipeline, logs, and artifacts:

```bash
zenml login --local
zenml up
```

The dashboard will be available at:

http://127.0.0.1:8237/


### 4️⃣ View pipeline runs

- Step execution times, input/output artifacts, and logs are visible per run.
- Useful for debugging, tracking experiments, or sharing results with the team.

### 5️⃣ Optional caching

- ZenML supports caching of steps for faster reruns.
- Caching can be enabled in run_pipeline.py via:

```bash
@pipeline(enable_cache=True)
```

### Stop the existing server (if needed)
```bash
zenml down
```
Or kill the daemon manually:
```bash
kill -9 9966
```
Then you can restart with:
```bash
zenml up
```
### If you want to run on a different port:
```bash
zenml up --port 8240
```
💡 **Recommendation**: Usually, after zenml login --local, you **don’t need to run** zenml up **multiple times** unless the daemon was stopped or crashed.




### 1️⃣ Install Required ZenML
In your venv virtual environment:
```bash
# Airflow orchestrator for pipelines
pip install "zenml[airflow]"

# S3 artifact store support
pip install "zenml[s3]"

# Docker deployment support
pip install "zenml[docker]"

# Optional: Kubernetes deployer if you want production-grade deployment
pip install "zenml[kubeflow]"

# Optional: HuggingFace deployment
pip install "zenml[huggingface]"

```
### 2️⃣ Register Components in ZenML
Orchestrator (Airflow)
```bash
zenml orchestrator register airflow_orchestrator --flavor=airflow
```
Artifact Store (S3)
```bash
zenml artifact-store register my_s3_store --flavor=s3 --path=s3://your-bucket-name
```
Replace your-bucket-name with your actual AWS S3 bucket. Make sure your AWS credentials are set in environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

Deployer (Docker for model APIs)
```bash
zenml deployer register docker_deployer --flavor=docker
```
Optional: HuggingFace Deployer
```bash
zenml deployer register huggingface_deployer --flavor=huggingface
```
For HuggingFace deployment, you also need a HuggingFace account and token:
```bash
export HF_TOKEN="your_hf_token"
```
### 3️⃣ Register & Activate a New Stack
```bash
zenml stack register my_job_stack \
  -o airflow_orchestrator \
  -a my_s3_store \
  -d docker_deployer

zenml stack set my_job_stack

```
✅ Check the stack:
```bash
zenml stack describe

```
It should show:
- Orchestrator: Airflow
- Artifact Store: S3
- Deployer: Docker

### 4️⃣ Run Your Pipeline
Make sure your pipeline script is set up as before:
```bash
python 4_Pipelines/run_pipeline.py
```
- Steps will run in Airflow
- Artifacts (datasets, models, logs) are stored in S3
- Docker deployment ready for the trained model

### 5️⃣ Deploy Your Model as a Live API
**Option A: Docker Deployment (fast and simple)**
```bash
# Register your model (after training in pipeline)
zenml model deployer register_model --stack my_job_stack --model_path=path/to/model --deployer docker_deployer --name taxi_demand_api

```
```bash
# Start the API
zenml model deploy start --name taxi_demand_api

```
- Your model will run as a **REST API locally**
- URL is usually: http://localhost:5000/predict

**Option B: HuggingFace Deployment (cloud, impressive)**
```bash
# Register the HuggingFace deployer
zenml deployer register huggingface_deployer --flavor=huggingface

# Deploy model to HuggingFace Spaces
zenml model deployer register_model --model_path=path/to/model --deployer huggingface_deployer --name taxi_demand_hf

```
- Users can call your model API directly from **HuggingFace cloud**
- Great for showing **international cloud ML skills** on your resume






```bash
```