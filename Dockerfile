FROM python:3.14.4-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=true

COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt
COPY hms.py .

CMD ["python", "/app/hms.py"]
