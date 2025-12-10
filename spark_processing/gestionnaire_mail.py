import os
import json
import smtplib
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_RECIPIENT = os.getenv("ALERT_RECIPIENT")

# sensor_id -> dernier état connu (localisation, etc.)
last_sensor_state = {}


def init_smtp_connection():
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASS)
    return server


server = init_smtp_connection()


def send_email_alert(alert, sensor_state, server):
    sensor_id = alert.get("sensor_id")
    alert_type = alert.get("alert_type")
    severity = alert.get("severity")
    message = alert.get("message")
    actual_value = alert.get("actual_value")
    threshold_value = alert.get("threshold_value")
    triggered_at = alert.get("triggered_at")

    building = sensor_state.get("building", "?") if sensor_state else "?"
    floor = sensor_state.get("floor", "?") if sensor_state else "?"
    room = sensor_state.get("room", "?") if sensor_state else "?"

    subject = f"[ALERTE {severity.upper()}] Capteur {sensor_id} - {alert_type}"

    body = (
        f"Alerte {severity} pour le capteur {sensor_id}.\n\n"
        f"Localisation : bâtiment {building}, étage {floor}, salle {room}.\n"
        f"Type d'alerte : {alert_type}\n"
        f"Message      : {message}\n"
        f"Valeur       : {actual_value} (seuil {threshold_value})\n"
        f"Déclenchée   : {triggered_at}\n"
    )

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_RECIPIENT

    server.send_message(msg)
    print(f"[MAIL] Alerte envoyée pour {sensor_id} ({severity})")


def process_iot_sensor_data(record_value):
    """Met à jour last_sensor_state à partir du topic iot-sensor-data."""
    sensor_id = record_value.get("sensor_id")
    location = record_value.get("location", {}) or {}
    metadata = record_value.get("metadata", {}) or {}

    if not sensor_id:
        return

    last_sensor_state[sensor_id] = {
        "building": location.get("building"),
        "floor": location.get("floor"),
        "room": location.get("room"),
        "battery_level": metadata.get("battery_level"),
        "signal_strength": metadata.get("signal_strength"),
        "timestamp": record_value.get("timestamp"),
        "value": record_value.get("value"),
        "unit": record_value.get("unit"),
        "sensor_type": record_value.get("sensor_type"),
    }


def process_iot_alert(record_value):
    """Traite une alerte venant de iot-alert et envoie un mail si besoin."""
    severity = record_value.get("severity")
    if severity not in ("critical", "warning"):
        return  # on ignore les info

    sensor_id = record_value.get("sensor_id")
    sensor_state = last_sensor_state.get(sensor_id)
    send_email_alert(record_value, sensor_state, server)


def main():
    consumer = KafkaConsumer(
        "iot-sensor-data",
        "iot-alert",
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest", 
        enable_auto_commit=True,
        group_id="alert-mail-consumer",
    )
    print("Alert mail consumer started (listening to iot-sensor-data & iot-alert)")

    for msg in consumer:
        topic = msg.topic
        value = msg.value

        if topic == "iot-sensor-data":
            process_iot_sensor_data(value)
        elif topic == "iot-alert":
            process_iot_alert(value)


if __name__ == "__main__":
    main()
