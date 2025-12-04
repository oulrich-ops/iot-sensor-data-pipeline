from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2025, 12, 4),
}

with DAG(
    dag_id="iot_sensor_pipeline",
    default_args=default_args,
    schedule="@once",     
    catchup=False,
    description="Pipeline IoT: simulateurs -> Kafka -> Spark -> Postgres + alert mail",
) as dag:


    # 1) Simulateur de capteurs (envoie vers iot-sensor-data)
    simulate_sensors = BashOperator(
        task_id="simulate_sensors",
        bash_command=(
            "cd /opt/iot-sensor-data-pipeline && "
            "python simulators/sensor_simulator.py"
        ),
    )

    # 2) Stream 1 : persistance brute (Spark -> Postgres)
    spark_data_persistence = BashOperator(
        task_id="spark_data_persistence",
        bash_command=(
            "cd /opt/iot-sensor-data-pipeline && "
            "python spark_processing/data_persistance.py"
        ),
    )


        # 3) Stream 2 : agrégation (Spark)
    spark_data_aggregation = BashOperator(
        task_id="spark_data_aggregation",
        bash_command=(
            "cd /opt/iot-sensor-data-pipeline && "
            "python spark_processing/data_agregator.py"
        ),
    )

    # 4) Stream 3 : détection d’alertes (Spark -> Postgres + topic iot-alert)
    spark_alert_detector = BashOperator(
        task_id="spark_alert_detector",
        bash_command=(
            "cd /opt/iot-sensor-data-pipeline && "
            "python spark_processing/alert_detector.py"
        ),
    )


        # 5) Gestionnaire d’alertes mail (consumer Kafka iot-alert)
    mail_manager = BashOperator(
            task_id="mail_manager",
            bash_command=(
                "cd /opt/iot-sensor-data-pipeline && "
                "python spark_processing/gestionnaire_mail.py"
            ),
        )

  
    simulate_sensors >> [
        spark_data_persistence,
        spark_data_aggregation,
        spark_alert_detector,
        mail_manager,
    ]
