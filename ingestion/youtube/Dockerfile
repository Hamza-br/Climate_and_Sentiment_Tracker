FROM python:3.11-slim

WORKDIR /app

COPY youtube_ingest.py /app/
COPY logging_utils.py /app/
COPY requirements.txt /app/
COPY .env.youtube /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "youtube_ingest.py"]
