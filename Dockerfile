FROM python:3.12-slim

EXPOSE 8000

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

WORKDIR /app

# --- SELECTIVE COPY FOR DEPLOYMENT ---
COPY f_API_Service/ ./f_API_Service/
COPY e_Pipelines/ ./e_Pipelines/
COPY configs/ ./configs/
COPY d_Data/artifacts/ ./d_Data/artifacts/
COPY logs.py .

# --- FIX: PERMISSIONS & DIRECTORIES ---
# 1. Create the appuser
# 2. Pre-create the ZenML config directory so the user owns it BEFORE the volume mounts
RUN adduser -u 5678 --disabled-password --gecos "" appuser && \
    mkdir -p /home/appuser/.config/zenml && \
    chown -R appuser:appuser /home/appuser /app

USER appuser

# --- FIX: TIMEOUT ---
# Added --timeout 300 to prevent Gunicorn from killing the worker during ZenML initialization
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "-k", "uvicorn.workers.UvicornWorker", "f_API_Service.b_main:app", "--timeout", "300"]