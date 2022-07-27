FROM apache/airflow:1.10.15-python3.8

RUN python -m pip install --upgrade pip

ENV DAGS_FOLDER=/opt/airflow/dags/repo/dags

COPY elaborate-baton-357506-f9435b87997e.json /usr/sa.json

USER airflow

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
