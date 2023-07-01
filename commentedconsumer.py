# Import the necessary classes from the Pulsar module
from pulsar import Client, MessageId, InitialPosition, ConsumerType

# Create a Pulsar client that connects to a local Pulsar server
client = Client('pulsar://localhost:6650')

# Create a Pulsar consumer that subscribes to 'my_topic' on the client
# The subscription_name is 'my-subscription'
# The consumer type is set to 'Exclusive', meaning only this consumer can consume messages from this subscription
# The initial position is set to 'Latest', meaning the consumer will start reading messages sent after it subscribes to the topic
consumer = client.subscribe('my_topic',
                            subscription_name='my-subscription',
                            consumer_type=ConsumerType.Exclusive,
                            initial_position=InitialPosition.Latest)

try:
    # An infinite loop that will continuously try to receive messages
    while True:
        # The consumer tries to receive a message
        # If there's no message, it will wait until a message arrives
        msg = consumer.receive()
        
        try:
            # If the message is successfully received, print its data and id
            print("Received message: '%s' id='%s'" % (msg.data(), msg.message_id()))
            
            # Acknowledge that the message has been successfully processed
            # This will inform the Pulsar server that the message doesn't need to be sent again
            consumer.acknowledge(msg)
        except:
            # If an error occurs during message processing, negatively acknowledge the message
            # This tells the Pulsar server to redeliver the message later
            consumer.negative_acknowledge(msg)

# The finally block will execute regardless of how the try block exits (whether it's due to an error or not)
finally:
    # Close the Pulsar client when done
    client.close()
