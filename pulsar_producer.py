from pulsar import Client
import json

client = Client('pulsar://localhost:6650')

producer = client.create_producer('my_topic')

with open('data/New York_0.json', 'r') as f:
    data = json.load(f)

producer.send(json.dumps(data).encode('utf-8'))
producer.flush()

producer.close()
client.close()
