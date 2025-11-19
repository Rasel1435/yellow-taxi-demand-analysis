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