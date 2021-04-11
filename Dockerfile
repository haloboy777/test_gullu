FROM python:3.8-buster

RUN pip install aiofiles aiocsv pymysql

COPY . /app

WORKDIR /app

CMD ["python", "app.py", "-i", "products.csv"]

