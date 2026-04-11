FROM python:3.14.4-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=true

RUN pip install hoymiles-wifi influxdb_client suntimes pyyaml
COPY hms.py .

CMD ["/app/hms.py"]
