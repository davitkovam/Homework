FROM python:3.11-slim-bullseye

WORKDIR /homework

COPY . .

RUN pip install --no-cache-dir -r requirements.txt



CMD ["python", "ETL_script.py"]