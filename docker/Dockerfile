FROM python:3.6

COPY requirements.txt docker/constraints-3.6.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt -c constraints-3.6.txt

ENTRYPOINT ["airflow"]
