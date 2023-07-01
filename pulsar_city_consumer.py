from pulsar import Client
import threading
import json

#  creating a consumer that subscribes to the city's topic
    #  client.subscribe() function takes two arguments: 
    #  1. topic name, which is formatted 'persistent://public/default/<topic_name>'
    #  2. subscription name 
    #  subscriptions allow multiple consumers to each receive a copy of the data. 
    #  using a single subscription shared between all cities.

# func to create and run a consumer for a given city

def run_consumer(city):
    client = Client('pulsar://localhost:6650') 

    consumer = client.subscribe(f'persistent://public/default/{city}_Restaurants', 'my-subscription')

    # main loop to keep listening for messages
    while True:
        msg = consumer.receive() # waiting to receive messages from the topic

        # once a message is received, try to process it
        try:
            #  print recieved message for each message, good for debugging
            #  we're decoding it to a string for printing
            print(f"Received message from {city}: '{msg.data().decode('utf-8')}'")
            
            # decoding the message data back to JSON and printing individual businesses

            data = json.loads(msg.data().decode('utf-8'))
            for business in data['businesses']:
                print(f"Received business: {business}")

            #  message processes sucessfully, acknowledge it

            consumer.acknowledge(msg)

        # except block for errors
        except:
            # if error occurs, negatively acknowledge the message
            consumer.negative_acknowledge(msg)

    #  this line will never be reached in this script, since the while loop runs forever

    client.close()


# using threading library to create consumers for New York and Los Angeles, each in their own thread
for city in ['New York', 'Los Angeles']:
    threading.Thread(target=run_consumer, args=(city,)).start()
