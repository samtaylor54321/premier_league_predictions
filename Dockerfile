FROM apache/airflow:2.1.2
COPY requirements.txt .
RUN mkdir /data
RUN mkdir /model
RUN mkdir /templates
RUN mkdir /static
ADD templates /opt/airflow/templates
ADD static /opt/airflow/static
RUN pip install -r requirements.txt
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libgomp1 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
EXPOSE 5000

