# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.12-slim

EXPOSE 8000

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

WORKDIR /app

# --- SELECTIVE COPY FOR DEPLOYMENT ---
# 1. Copy the core code folders
COPY f_API_Service/ ./f_API_Service/
COPY e_Pipelines/ ./e_Pipelines/
COPY configs/ ./configs/

# 2. Copy the essential models/artifacts
COPY d_Data/artifacts/ ./d_Data/artifacts/

# 3. Copy the individual log utility file (Fixes the ModuleNotFoundError)
COPY logs.py .
# -------------------------------------

# Setup user permissions
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Start the API
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "-k", "uvicorn.workers.UvicornWorker", "f_API_Service.b_main:app"]