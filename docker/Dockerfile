FROM python:3.8.12

COPY requirements_local.txt requirements_airflow.txt ./
RUN pip install --upgrade pip && \
    pip install \
    -r requirements_local.txt \
    -r requirements_airflow.txt

ENTRYPOINT ["airflow"]
