from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

with open('data/New York_0.json', 'r') as f:
    data = json.load(f)

producer.send('my_topic', data)
producer.flush()

