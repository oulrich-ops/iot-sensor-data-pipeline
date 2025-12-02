import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

class IoTSensorSimulator:
    def __init__(self, kafka_servers, sensor_id, sensor_type):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        
    def generate_reading(self):
        base_values = {
            'temperature': (15, 30),
            'humidity': (30, 70),
            'pressure': (980, 1020)
        }
        
        min_val, max_val = base_values.get(self.sensor_type, (0, 100))
        value = random.uniform(min_val, max_val)
        
        # Simulation d'anomalies (5% de chance)
        if random.random() < 0.05:
            value = random.uniform(max_val, max_val * 1.5)
        
        return {
            'sensor_id': self.sensor_id,
            'sensor_type': self.sensor_type,
            'location': {
                'building': f'Building_{random.choice(["A", "B", "C"])}',
                'floor': random.randint(1, 5),
                'room': f'Room_{random.randint(100, 500)}'
            },
            'timestamp': datetime.utcnow().isoformat(),
            'value': round(value, 2),
            'unit': self.get_unit(),
            'metadata': {
                'battery_level': random.randint(50, 100),
                'signal_strength': random.randint(-80, -30)
            }
        }
    
    def get_unit(self):
        units = {
            'temperature': 'celsius',
            'humidity': 'percent',
            'pressure': 'hPa'
        }
        return units.get(self.sensor_type, 'unknown')
    
    def run(self, interval=5):
        while True:
            reading = self.generate_reading()
            self.producer.send('iot-sensor-data', reading)
            print(f"Sent: {reading}")
            time.sleep(interval)

if __name__ == '__main__':
    simulator = IoTSensorSimulator(
        kafka_servers=['kafka:9092'],
        sensor_id='sensor_temp_001',
        sensor_type='temperature'
    )
    simulator.run()