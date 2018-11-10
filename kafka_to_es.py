import sys
from datetime import datetime
import logging
import json
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = 'country_twitter_stream2'
MAX_POLL_RECORDS = 10
tweets_return = []

es = Elasticsearch()

# To consume messages
consumer = KafkaConsumer('test', group_id="es_group",
                          auto_commit_enable=True,
                          auto_commit_interval_ms=30 * 1000,
                          auto_offset_reset='smallest',
                          bootstrap_servers=['localhost:9092'])

consumer = KafkaConsumer(TOPICS, bootstrap_servers=BOOTSTRAP_SERVERS, auto_offset_reset='earliest',
                         consumer_timeout_ms=30000)

esid = 0

for message in consumer:
    time.sleep(1)
    print "next"
    esid += 1
    if esid % 1000 == 0:
      print esid

    msg = json.loads(message.value)

    if not 'index' in msg:
      print "you must specify the index name in the json wrapper"
      sys.exit(-1)

    index = msg['index']

    if 'schema' in msg:
      esid = 1
      print "Switching to Index", index
      if es.indices.exists(index=index):
        print es.indices.delete(index=index)
      print es.indices.create(index=index )
      print es.indices.put_mapping(index=index, doc_type= msg['doc_type'], body=msg['schema']['mappings'] )

    es.index(index=index, doc_type= msg['doc_type'], id=esid, body=msg['body'])