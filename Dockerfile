FROM python:3.11

WORKDIR /app
COPY . /app

RUN pip install fastmcp psycopg2-binary

CMD ["python", "server.py"]

