from kafka import KafkaConsumer
import json
import traceback

#  consume latest messages and auto-commit offsets
try:
    consumer = KafkaConsumer('my_topic',
                             bootstrap_servers='localhost:9092',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             consumer_timeout_ms=6000,
                             enable_auto_commit=False)
    
    print("Consumer created, start consuming")
    for message in consumer:
        print(message.value)

except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Exception Type: {type(e).__name__}")
    traceback.print_exc()
finally:
    print("Closing the consumer")
    consumer.close()
