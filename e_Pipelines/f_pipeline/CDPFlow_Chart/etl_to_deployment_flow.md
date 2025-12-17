# ETL to Deployment Pipeline Flow

```mermaid
flowchart TD
    A[Raw Data Source] -->|Ingest| B[Ingest Data]
    B -->|Clean| C[Clean Data]
    C -->|Feature Engineering| D[Feature Engineering]
    D -->|Select Features| E[Select Best Features]
    E -->|Scale/Normalize| F[Scale Features]
    F -->|Dimensionality Reduction| G[Reduce Dimensionality]
    G --> H[Split Data]
    H --> I[Train RandomForest Model]
    I --> J[Evaluate Model]
    J --> K[Trigger Deployment]
    K -->|If thresholds met| L[Deploy Model to MLflow]

    F -->|Output scaler path| I
    G -->|Output PCA path| I
    J -->|Metrics (R2, MAPE)| K
    L -->|Custom Tags| MLflow
```

## Description

- **A → G:** ETL & feature pipeline steps to prepare data.
- **H:** Split data into training and testing sets.
- **I:** Train the RandomForest model using training data and log model, scaler, PCA to MLflow.
- **J:** Evaluate model on test set to compute metrics (R2, MAPE).
- **K:** Decide deployment based on thresholds.
- **L:** Deploy the model to MLflow if thresholds are met, including custom tags for scaler, PCA, and metrics.

This diagram shows the full end-to-end workflow from raw data ingestion to automated deployment with MLflow integration.

