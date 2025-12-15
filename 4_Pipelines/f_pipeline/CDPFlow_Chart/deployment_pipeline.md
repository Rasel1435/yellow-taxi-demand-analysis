# Deployment Pipeline Flow

```mermaid
flowchart TD
    A[Processed Data (from ETL)] -->|Split Data| B[Train/Test Split]
    B --> C[Train RandomForest Model]
    C --> D[Evaluate Model]
    D --> E[Trigger Deployment]
    E -->|If thresholds met| F[Deploy Model to MLflow]

    C -->|Input scaler path| C
    C -->|Input PCA path| C
    D -->|Metrics (R2, MAPE)| E
    F -->|Custom Tags (scaler, PCA, metrics)| MLflow
```

## Description

* **A → B:** Take processed data from ETL pipeline and split into training and testing sets.
* **C:** Train the RandomForest model:
  - Use training data (`X_train`, `y_train`).
  - Log the trained model, scaler, and PCA objects to MLflow.
  - Hyperparameter tuning using `RandomizedSearchCV`.
* **D:** Evaluate the trained model on the test set:
  - Compute metrics such as R2 and MAPE.
* **E:** Trigger deployment decision:
  - Deploy only if evaluation metrics meet thresholds:
    - `R2 >= min_r2` (default 0.92)
    - `MAPE <= max_mape` (default 0.45)
* **F:** Deploy the model to MLflow if thresholds are met:
  - Include custom tags for scaler, PCA, R2, and MAPE metrics.
  - Optionally version the model for serving or further monitoring.