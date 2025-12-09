import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import math

# === Configuration des bâtiments, étages et pièces ===
buildings = ["A"]
floors = range(1, 3)
rooms = range(100, 104)
sensor_types = ["temperature", "humidity", "pressure"]

# === Classe représentant un capteur ===
class IoTSensor:
    def __init__(self, sensor_id, sensor_type, building, floor, room):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.building = building
        self.floor = floor
        self.room = room
        self.phase = random.uniform(0, 2 * math.pi)
        # Valeurs cibles réalistes
        self.target_temp = random.uniform(20, 25)
        self.target_humidity = random.uniform(40, 55)
        self.target_pressure = random.uniform(1010, 1020)

    def generate_reading(self):
        now = time.time()

        if self.sensor_type == "temperature":
            # base "normale"
            base_value = self.target_temp + 1 * math.sin((now / 60) * 2 * math.pi + self.phase)
            anomaly_chance = random.random()
            if anomaly_chance < 0.10:
                # 10% des mesures vraiment critiques > 35°C
                value = random.uniform(36.0, 40.0)
            elif anomaly_chance < 0.40:
                # 30% proches du seuil haut (28–35°C) -> bcp de warnings/alerts
                value = random.uniform(28.0, 35.0)
            else:
                # reste du temps, valeur autour de la cible
                value = base_value + random.uniform(-0.2, 0.2)

        elif self.sensor_type == "humidity":
            base_value = self.target_humidity + 2 * math.sin((now / 90) * 2 * math.pi + self.phase)
            anomaly_chance = random.random()
            if anomaly_chance < 0.15:
                # 15% trop bas < 20%
                value = random.uniform(10.0, 19.0)
            elif anomaly_chance < 0.30:
                # 15% trop haut > 70%
                value = random.uniform(71.0, 85.0)
            else:
                value = base_value + random.uniform(-1.0, 1.0)

        elif self.sensor_type == "pressure":
            base_value = self.target_pressure + 1.5 * math.sin((now / 120) * 2 * math.pi + self.phase)
            anomaly_chance = random.random()
            if anomaly_chance < 0.10:
                # 10% très bas < 950 hPa
                value = random.uniform(930.0, 949.0)
            elif anomaly_chance < 0.20:
                # 10% très haut > 1050 hPa
                value = random.uniform(1051.0, 1070.0)
            else:
                value = base_value + random.uniform(-0.5, 0.5)

        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "location": {
                "building": self.building,
                "floor": self.floor,
                "room": self.room,
            },
            "timestamp": datetime.utcnow().isoformat(),
            "value": round(value, 2),
            "unit": self.get_unit(),
            "metadata": {
                # augmente la proba de low_battery (<30%)
                "battery_level": random.choices(
                    population=[random.randint(10, 29), random.randint(30, 100)],
                    weights=[0.3, 0.7],
                )[0],
                "signal_strength": random.randint(-80, -30),
            },
        }

    def get_unit(self):
        units = {"temperature": "celsius", "humidity": "percent", "pressure": "hPa"}
        return units[self.sensor_type]


# === Création du producer Kafka ===
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# === Création de tous les capteurs ===
sensors = []
for b in buildings:
    for f in floors:
        for r in rooms:
            for t in sensor_types:
                sensor_id = f"{b}_{f}_{r}_{t}"
                sensors.append(IoTSensor(sensor_id, t, b, f, r))

print(f"Total sensors: {len(sensors)}")

# === Boucle de simulation ===
while True:
    for sensor in sensors:
        reading = sensor.generate_reading()
        producer.send("iot-sensor-data", reading)
        print(f"Sent: {reading}")
    time.sleep(0.1)
