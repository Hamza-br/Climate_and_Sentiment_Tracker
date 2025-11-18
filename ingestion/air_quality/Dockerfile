FROM python:3.11-slim

WORKDIR /app

COPY aq_ingest.py /app/
COPY logging_utils.py /app/
COPY requirements.txt /app/
COPY .env.openaq /app/

RUN pip install --no-cache-dir -r requirements.txt python-dotenv==1.0.1

CMD ["python", "aq_ingest.py"]