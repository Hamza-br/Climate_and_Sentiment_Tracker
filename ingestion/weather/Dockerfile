FROM python:3.11-slim

WORKDIR /app

COPY weather_ingest.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "weather_ingest.py"]
