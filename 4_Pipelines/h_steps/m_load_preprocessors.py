from zenml import step
from typing import Tuple, Annotated
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import joblib

@step(enable_step_logs=True)
def load_preprocessors(
    scaler_path: str,
    pca_path: str
) -> Tuple[
    Annotated[StandardScaler, "scaler"],
    Annotated[PCA, "pca"]
]:
    scaler = joblib.load(scaler_path)
    pca = joblib.load(pca_path)
    return scaler, pca



# It’s all about consistency and reusability of preprocessing steps.