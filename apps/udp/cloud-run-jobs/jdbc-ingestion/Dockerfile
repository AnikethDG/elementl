FROM python:3.11-slim
WORKDIR /app
COPY jdbc-ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY jdbc-ingestion/main.py .
ENTRYPOINT ["python3", "main.py"]
