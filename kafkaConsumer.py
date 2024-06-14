from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import json
from s3fs import S3FileSystem

# creating consumer object

consumer = KafkaConsumer(
            'kafkaTopicName',
            bootstrap_servers=['x.x.x.x:9092'],
            value_deserializer=lambda x: loads(x.decode('utf-8')))

# manual test = contintually outputs whatever is fed into the producer

# for c in consumer:
#     print(c.value)

# creating the s3 object

s3 = S3FileSystem(key='###', secret='###')

# throwing the file into s3 bucket with a different filename each time

for count, i in enumerate(consumer):
    # print(count) # keeps a running count of the consumer object size
    # print(i.value) # shows the value at the i index
    with s3.open("s3://bucketHere/stonks_{}.json". format(count), 'w') as file:
        json.dump(i.value, file)