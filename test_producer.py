from kafka import KafkaProducer
import json

# Initialize the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Send some messages
for i in range(10):
    message = {"message": f"This is message {i}"}
    producer.send('test', value=message)

# Ensure all messages are sent
producer.flush()
