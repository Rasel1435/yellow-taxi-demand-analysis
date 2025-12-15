from zenml import step
from typing import Annotated
import pandas as pd
from sklearn.base import BaseEstimator

@step(enable_step_logs=True)
def predict(
    model: Annotated[BaseEstimator, "deployed_model"],
    X: Annotated[pd.DataFrame, "features"]
) -> Annotated[pd.Series, "predictions"]:
    return model.predict(X)



"""
    The n_predict.py step is all about taking your trained model and new, preprocessed data to
    generate predictions.

    Combined with load_preprocessors.py, it allows you to reuse the same model, scaler, and PCA
    without retraining.

    Essentially, it makes your ML workflow reusable and modular—you can take any new raw data, run it
    through your preprocessing + feature steps, then call predict to get predictions.
"""