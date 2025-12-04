FROM apache/airflow:3.0.0

USER root

RUN mkdir -p /opt/iot-sensor-data-pipeline
COPY . /opt/iot-sensor-data-pipeline
RUN chown -R 50000:0 /opt/iot-sensor-data-pipeline

USER 50000:0

RUN pip install --no-cache-dir -r /opt/iot-sensor-data-pipeline/requirements.txt
