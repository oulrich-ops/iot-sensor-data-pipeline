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


last_sensor_state = {}

def init_smtp_connection():
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASS)
    return server

server = init_smtp_connection()
def send_email_alert(alert, server):
    sensor_id = alert.get("sensor_id")
    alert_type = alert.get("alert_type")
    severity = alert.get("severity")
    message = alert.get("message")
    actual_value = alert.get("actual_value")
    threshold_value = alert.get("threshold_value")
    triggered_at = alert.get("triggered_at")

    # location et metadata viennent directement du message iot-alert
    location = alert.get("location", {}) or {}
    metadata = alert.get("metadata", {}) or {}

    building = location.get("building", "?")
    floor = location.get("floor", "?")
    room = location.get("room", "?")

    battery_level = metadata.get("battery_level")
    signal_strength = metadata.get("signal_strength")

    subject = f"[ALERTE {severity.upper()}] Capteur {sensor_id} - {alert_type}"

    body_lines = [
        f"Alerte {severity} pour le capteur {sensor_id}.",
        "",
        f"Localisation : bâtiment {building}, étage {floor}, salle {room}.",
        f"Type d'alerte : {alert_type}",
        f"Message      : {message}",
        f"Valeur       : {actual_value} (seuil {threshold_value})",
        f"Déclenchée   : {triggered_at}",
    ]

    # Infos bonus si présentes
    if battery_level is not None:
        body_lines.append(f"Niveau batterie : {battery_level}%")
    if signal_strength is not None:
        body_lines.append(f"Signal         : {signal_strength} dBm")

    body = "\n".join(body_lines)

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_RECIPIENT

    server.send_message(msg)
    print(f"[MAIL] Alerte envoyée pour {sensor_id} ({severity})")


def process_iot_alert(record_value, server):
    """Traite une alerte venant de iot-alert et envoie un mail si besoin."""
    severity = record_value.get("severity")
    if severity not in ("critical", "warning"):
        return  # on ignore les info

    send_email_alert(record_value, server)


def main():
    server = init_smtp_connection()

    consumer = KafkaConsumer(
        "iot-alert",
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="alert-mail-consumer",
    )
    print("Alert mail consumer started (listening only to iot-alert)")

    for msg in consumer:
        value = msg.value
        process_iot_alert(value, server)


if __name__ == "__main__":
    main()
