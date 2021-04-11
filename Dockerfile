FROM python:3.8-slim

RUN pip install aiofiles aiocsv mysql-connector-python cryptography

COPY . /app

WORKDIR /app

CMD ["python", "app.py", "-i", "products.csv"]

