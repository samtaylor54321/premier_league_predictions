FROM apache/airflow:2.1.2
COPY requirements.txt .
RUN mkdir /data
RUN mkdir /model
RUN mkdir /templates
RUN mkdir /static
ADD templates /opt/airflow/templates
ADD static /opt/airflow/static
RUN pip install -r requirements.txt

EXPOSE 5000