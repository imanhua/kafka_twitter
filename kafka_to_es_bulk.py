import sys
import time
from datetime import datetime
import logging
import json
from elasticsearch import Elasticsearch,helpers
from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = 'country_twitter_stream_output2'
MAX_POLL_RECORDS = 10
INDEX_NAME = "otravo"
DOC_TYPE = "assignment"
tweets_return = []

API_ENDPOINTS = 'https://544c1289c9ec450889b30cc9ce45524c.us-east-1.aws.found.io:9243'

USER_NAME = 'elastic'
PASSWORD = 'tk04BsMzyXHEYGP5BkBXsfV1'
# es = Elasticsearch()
es = Elasticsearch([API_ENDPOINTS],
                   http_auth=[USER_NAME, PASSWORD],
                   scheme="https"
                   )
#
# # To consume messages
# consumer = KafkaConsumer('test', group_id="es_group",
#                           auto_commit_enable=True,
#                           auto_commit_interval_ms=30 * 1000,
#                           auto_offset_reset='smallest',
#                           bootstrap_servers=['localhost:9092'])

consumer = KafkaConsumer(TOPICS, bootstrap_servers=BOOTSTRAP_SERVERS, auto_offset_reset='earliest',
                         consumer_timeout_ms=30000)


def get_message():
    esid = 0

    for message in consumer:
        # time.sleep(1)
        # print("next")
        esid += 1
        if esid % 1000 == 0:
          print(esid)
        # print("message")
        # print(message)
        # print(".value")
        # print(message.value)
        # print("json")
        msg = json.loads(message.value)
        # print(type(msg))

        insert_to_es(esid, msg)
        # if not 'index' in msg:
        #   print("you must specify the index name in the json wrapper")
        #   sys.exit(-1)


def insert_to_es(esid, msg):

    es.index(index=INDEX_NAME, doc_type= DOC_TYPE, id=esid, body=msg)
    # helpers.bulk(es, msg, index=INDEX_NAME, doc_type=DOC_TYPE)
    print('%s One record inserted ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))

    # helpers.bulk(es, msg, index=INDEX_NAME, doc_type=DOC_TYPE)

    # if 'schema' in msg:
    #   esid = 1
    #   print "Switching to Index", index
    # if es.indices.exists(index=index):
    #   print es.indices.delete(index=index)

    # print es.indices.put_mapping(index=index, doc_type= msg['doc_type'], body=msg['schema']['mappings'] )


def drop_index():
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    print('%s index dropped ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def create_index():
    # print('%s create new index ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    es.indices.create(index=INDEX_NAME)
    print('%s index dropped ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    drop_index()
    create_index()
    get_message()
