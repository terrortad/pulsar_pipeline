from pulsar import Client
import json

import time

def pulsar_producer_function(city): # function defined
    client = Client('pulsar://localhost:6650')

    producer = client.create_producer('my_topic')

    with open('data/New York_350.json', 'r') as f:
        data = json.load(f)

    producer.send(json.dumps(data).encode('utf-8'))
    producer.flush()

    # wait for 1 second before closing
    time.sleep(1)

    producer.close()
    client.close()

# choose which city by calling function and argument
pulsar_producer_function("New York")
