from pulsar import Client, Message
import json
import os

# func to publish data to Pulsar
def pulsar_producer_function(city):

    client = Client('pulsar://localhost:6650')

    # path to data
    data_path = "data"
    
    # filename format
    filename_format = "{}_{}.json"

    #  loop over all JSON files in city
    #  i = 0 starts from fist file, which each file is indexed by 50
    i = 0
    while True:
        file_path = os.path.join(data_path, filename_format.format(city, i))
        
        #  if the file doesn't exist, exit the loop
        if not os.path.exists(file_path):
            print(f"No file found at {file_path}. Skipping...")
            break

        #  open file and read the data
        with open(file_path, 'r') as f:
            data = json.load(f)

        #  define the producer using the city name as the topic
        producer = client.create_producer(f"persistent://public/default/{city}_Restaurants")

        # send the data
        print(f"Sending data for {city}: {data}")
        producer.send(json.dumps(data).encode('utf-8'))

        for business in data['businesses']:
            print("Received business: '%s'" % business) #  print "recieved business" for each business

        #  close the producer
        producer.close()

        #  move to the next file (increments by 50)
        i = 50 if i == 0 else i + 50

    client.close()

#  calling the function for each desired city
for city in ['New York', 'Los Angeles']:
    pulsar_producer_function(city)
