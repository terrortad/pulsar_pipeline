from pulsar import Client, MessageId, InitialPosition, ConsumerType

client = Client('pulsar://localhost:6650')

consumer = client.subscribe('my_topic',
                            subscription_name='my-subscription',
                            consumer_type=ConsumerType.Exclusive,
                            initial_position=InitialPosition.Latest)

try:
    while True:
        msg = consumer.receive()
        try:
            print("Received message: '%s' id='%s'" % (msg.data(), msg.message_id()))
            # Acknowledge successful processing of the message
            consumer.acknowledge(msg)
        except:
            # Message failed to process, redeliver later
            consumer.negative_acknowledge(msg)
finally:
    client.close()
