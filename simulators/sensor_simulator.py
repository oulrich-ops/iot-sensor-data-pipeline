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


class IoTSensor:
    def __init__(self, sensor_id, sensor_type, building, floor, room):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.building = building
        self.floor = floor
        self.room = room
        self.phase = random.uniform(0, 2 * math.pi)
         
        self.target_temp = random.uniform(20, 25)
        self.target_humidity = random.uniform(40, 55)
        self.target_pressure = random.uniform(1010, 1020)

    def generate_reading(self):
        now = time.time()
        
        
        signal_chance = random.random()
        if signal_chance < 0.002:  # Critical 
            signal_strength = random.randint(-90, -76)
        elif signal_chance < 0.006:  # Warning 
            signal_strength = random.randint(-75, -71)
        else:  # 99.4% - Normal
            signal_strength = random.randint(-70, -40)

        
        battery_chance = random.random()
        if battery_chance < 0.001:  #  Critical 
            battery_level = random.randint(5, 19)
        elif battery_chance < 0.003:  #   Warning  
            battery_level = random.randint(20, 39)
        else:  # 99.7% - Normal
            battery_level = random.randint(40, 100)

        
        if self.sensor_type == "temperature":
            base_value = self.target_temp + 1 * math.sin((now / 60) * 2 * math.pi + self.phase)
            anomaly_chance = random.random()
            
            if anomaly_chance < 0.001:  #  CRITICAL  
                if random.random() < 0.7:  # 70% trop chaud, 30% trop froid
                    value = random.uniform(30.1, 35)
                else:
                    value = random.uniform(10, 14.9)
            elif anomaly_chance < 0.004:  # 0.3% - WARNING (27-30°C)
                value = random.uniform(27.1, 29.9)
            else:  #  Normal
                value = base_value + random.uniform(-0.5, 0.5)
        
        
        elif self.sensor_type == "humidity":
            base_value = self.target_humidity + 2 * math.sin((now / 90) * 2 * math.pi + self.phase)
            anomaly_chance = random.random()
            
            if anomaly_chance < 0.001:  #  CRITICAL
                if random.random() < 0.5:
                    value = random.uniform(15, 29.9)  # Trop sec
                else:
                    value = random.uniform(70.1, 85)  # Trop humide
            elif anomaly_chance < 0.004:  #   WARNING 
                if random.random() < 0.5:
                    value = random.uniform(30, 34.9)  # Sec
                else:
                    value = random.uniform(60.1, 69.9)  # Humide
            else:  #  Normal
                value = base_value + random.uniform(-1, 1)
        
         
        elif self.sensor_type == "pressure":
            base_value = self.target_pressure + 1.5 * math.sin((now / 120) * 2 * math.pi + self.phase)
            anomaly_chance = random.random()
            
            if anomaly_chance < 0.0015:  #   CRITICAL 
                if random.random() < 0.5:
                    value = random.uniform(950, 979.9)  # Très bas
                else:
                    value = random.uniform(1040.1, 1060)  # Très haut
            elif anomaly_chance < 0.005:  #  WARNING  
                if random.random() < 0.5:
                    value = random.uniform(980, 994.9)  # Bas
                else:
                    value = random.uniform(1030.1, 1039.9)  # Haut
            else:  # 99.5% - Normal
                value = base_value + random.uniform(-0.5, 0.5)

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
                "battery_level": battery_level,
                "signal_strength": signal_strength
            }
        }

    def get_unit(self):
        units = {'temperature': 'celsius', 'humidity': 'percent', 'pressure': 'hPa'}
        return units[self.sensor_type]

# === Création du producer Kafka ===
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Création de tous les capteurs 
sensors = []
for b in buildings:
    for f in floors:
        for r in rooms:
            for t in sensor_types:
                sensor_id = f"{b}_{f}_{r}_{t}"
                sensors.append(IoTSensor(sensor_id, t, b, f, r))

print(f"Total sensors: {len(sensors)}")


while True:
    for sensor in sensors:
        reading = sensor.generate_reading()
        producer.send("iot-sensor-data", reading)
        print(f"Sent: {reading}")
        
        #time.sleep(random.uniform(3, 6))  
    time.sleep(3)
