import re
import logging
import json
import pycountry
from kafka import KafkaConsumer
from textblob import TextBlob

BOOTSTRAP_SERVERS = 'localhost:9092'
# TOPICS = 'wks-debug-example-topic-two'
TOPICS = 'country_twitter_stream_output'
# TOPICS = 'country_twitter_stream2'
MAX_POLL_RECORDS = 10

'''
consumer = KafkaConsumer(TOPICS,
                         bootstrap_servers=BOOTSTRAP_SERVERS,
                         enable_auto_commit='False',
                         max_poll_records=MAX_POLL_RECORDS,   # 500 by default
                         consumer_timeout_ms=30000)   # stop if no message after 30 seconds
'''
# consumer = KafkaConsumer(TOPICS, bootstrap_servers=BOOTSTRAP_SERVERS, enable_auto_commit='False', max_poll_records=10)
consumer = KafkaConsumer(TOPICS, bootstrap_servers=BOOTSTRAP_SERVERS, auto_offset_reset='earliest',
                         consumer_timeout_ms=30000)
# metrics = consumer.metrics()
# print(metrics)
# logging.info('Start reading')]

count = 0
for message in consumer:
    count += 1
    # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                      message.offset, message.key,
    #                                      message.value))
    new_message = json.loads(message.value)
    type_1 = type(new_message)
    print(type_1, "\n")
    # for country in pycountry.countries:
    #     if country.name in clean_message."text"
    #         print(country.name)


    # print(message.partition)
    # print(count, '  ', new_message["text"], "\n")
    if count % MAX_POLL_RECORDS == 0:
        logging.warning("received %d records" % MAX_POLL_RECORDS)
        consumer.commit
        logging.warning('offsets have been committed')

consumer.commit
logging.warning('Consumer stopped')
logging.warning('offsets have been committed')