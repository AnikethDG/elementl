FROM python:3.11-slim
WORKDIR /app
COPY bulk-ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY bulk-ingestion/clients/ clients/
COPY bulk-ingestion/main.py .
ENTRYPOINT ["python3", "main.py"]
