# No slim since we need to build psutil (at least on Macbook M1).
FROM python:3.8.12

RUN pip install --upgrade pip

COPY . .

COPY requirements*.txt ./

RUN pip install -r requirements_parse.txt

ENV PYTHONPATH=/dags

ENTRYPOINT ["python", "run_parse_dataset_folder.py"]
