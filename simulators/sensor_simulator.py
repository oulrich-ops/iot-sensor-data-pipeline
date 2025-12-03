import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# === Configuration des bâtiments, étages et pièces ===
buildings = ["A", "B", "C"]
floors = range(1, 6)
rooms = range(100, 106)  # quelques rooms par étage pour l'exemple
sensor_types = ["temperature", "humidity", "pressure"]

# === Classe représentant un capteur ===
class IoTSensor:
    def __init__(self, sensor_id, sensor_type, building, floor, room):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.building = building
        self.floor = floor
        self.room = room

    def generate_reading(self):
        base_values = {
            'temperature': (15, 30),
            'humidity': (30, 70),
            'pressure': (980, 1020)
        }
        min_val, max_val = base_values[self.sensor_type]
        value = random.uniform(min_val, max_val)

        # Simulation d'anomalies (5% chance)
        if random.random() < 0.05:
            value = random.uniform(max_val, max_val * 1.5)

        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "location": {
                "building": self.building,
                "floor": self.floor,
                "room": self.room
            },
            "timestamp": datetime.utcnow().isoformat(),
            "value": round(value, 2),
            "unit": self.get_unit(),
            "metadata": {
                "battery_level": random.randint(50, 100),
                "signal_strength": random.randint(-80, -30)
            }
        }

    def get_unit(self):
        units = {'temperature': 'celsius', 'humidity': 'percent', 'pressure': 'hPa'}
        return units[self.sensor_type]

# === Création du producer Kafka ===
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # 'kafka:9092' si dans un conteneur Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
    time.sleep(5)  
