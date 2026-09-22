FROM python:3.11-slim
WORKDIR /app
COPY arcgis-ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY arcgis-ingestion/clients/ clients/
COPY arcgis-ingestion/main.py .
ENTRYPOINT ["python3", "main.py"]
