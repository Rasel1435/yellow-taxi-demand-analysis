# Training Pipeline Flow

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
    J --> K[Save Model Artifact]

    F -->|Output scaler path| I
    G -->|Output PCA path| I
    J -->|Metrics (R2, MAPE)| K
```


## Description

*   **A → G:** ETL & feature pipeline steps to prepare data.
*   **H:** Split data into training and testing sets.
*   **I:** Train the RandomForest model using training data and log model, scaler, PCA to MLflow.
*   **J:** Evaluate model on test set to compute metrics (R2, MAPE).
*   **K:** Save the trained model artifact for inference or deployment.