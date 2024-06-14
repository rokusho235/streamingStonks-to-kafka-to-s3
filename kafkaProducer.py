import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json

# creating the producer obj to send data to Kafka

producer = KafkaProducer(bootstrap_servers=['x.x.x.x:9092'],
                            value_serializer=lambda x: # formats data into json format
                            dumps(x).encode('utf-8'))

# manually test by sending data from here to the EC2 Kafka consumer

producer.send('kafkaTopicName', value="{'1':'seventey','maybe'}")

# simulating stock market with existing data--can plug in actual stock API here too

df = pd.read_csv("indexProcessed.csv")

# randomly pulling a row and then outputting into json format
# then feeding that data to Kafka

while True:
    stock_dict = df.sample(1).to_dict(orient="records")[0] # orient gets rid of the extra index# thing
    producer.send('kafkaTopicName', value=stock_dict)
    sleep(60) # 1 minute pause

# to remove data from kafka and clean out the consumer

producer.flush()