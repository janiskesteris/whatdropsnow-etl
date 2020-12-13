FROM python:3.8
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
COPY db_config.docker.yml db_config.yml
CMD python etl.py
CMD python sql.py