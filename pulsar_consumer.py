from pulsar import Client, MessageId, InitialPosition, ConsumerType
import json

#  creates a Pulsar consumer that subscribes to 'my_topic' on the client
#  subscription_name is 'my-subscription'
#  consumer type is set to 'Exclusive', meaning only this consumer can consume messages from this subscription
#  initial position is set to 'Latest', meaning the consumer will start reading messages sent after it subscribes to the topic


client = Client('pulsar://localhost:6650')

consumer = client.subscribe('my_topic',
                            subscription_name='my-subscription',
                            consumer_type=ConsumerType.Exclusive,
                            initial_position=InitialPosition.Latest)

try:
    #  infinite loop that will continuously try to receive messages
   while True:
    msg = consumer.receive()
    
    try:
        #  parse the message data into a Python object
        data = json.loads(msg.data())
        
        #  iterate over the businesses in the data
        for business in data['businesses']:
            print("Received business: '%s'" % business) #  print Recieved business for each business in data
            
        consumer.acknowledge(msg)
    except:
        consumer.negative_acknowledge(msg)

#  finally block will close program regardless of how the try block exits (whether it's due to an error or not)
finally:
    client.close()
