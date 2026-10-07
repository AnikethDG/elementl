FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py common.py ./
COPY transforms ./transforms

RUN useradd --create-home --uid 10001 silver && chown -R silver:silver /app
USER silver

ENTRYPOINT ["python", "main.py"]
