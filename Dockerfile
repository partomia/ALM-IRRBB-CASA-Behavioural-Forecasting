# Streamlit app only (no torch). Runs anywhere: reads parquet exports or CDW
# Impala, and calls the CAI model endpoint for stress what-ifs (falls back to a
# naive in-app model if no endpoint is set).
#
#   docker build -t casa-alm-app .
#   docker run -p 8501:8501 -v $PWD/data/parquet:/app/data/parquet casa-alm-app
#   docker run -p 8501:8501 -e CASA_STORAGE_BACKEND=impala -e CASA_IMPALA_HOST=... \
#       -e CASA_IMPALA_USER=... -e CASA_IMPALA_PASSWORD=... -e CASA_ENDPOINT_URL=... casa-alm-app
FROM python:3.11-slim

WORKDIR /app
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

COPY casa/ casa/
COPY app/ app/
COPY config/ config/

ENV CASA_STORAGE_BACKEND=parquet \
    CASA_APP_HOST=0.0.0.0 \
    CDSW_APP_PORT=8501
EXPOSE 8501
CMD ["python", "app/run.py"]
