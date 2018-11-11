from datetime import datetime
import json
from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer
import readconfig

kafka_config = readconfig.read_config('kafka_config')
bootstrap_servers = kafka_config['bootstrap_servers']
# # watch out! This has to be the filtered topic name
topics = kafka_config['filtered_topics']

es_config = readconfig.read_config('es_config')
index_name = es_config['index_name']
# index_name = "otravo_test"
doc_type = es_config['doc_type']

MAX_POLL_RECORDS = 500
CONSUMER_TIMEOUT_MS = 60 * 60 * 1000

consumer = KafkaConsumer(topics, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest',
                         enable_auto_commit='False',
                         consumer_timeout_ms=CONSUMER_TIMEOUT_MS)


def connect_to_es(config):

    es_config_inner = readconfig.read_config(config)
    api_endpoints = es_config_inner['api_endpoints']
    user_name = es_config_inner['user_name']
    password = es_config_inner['password']
    # print(api_endpoints, user_name, password)
    es = Elasticsearch([api_endpoints],
                       http_auth=[user_name, password],
                       scheme="https"
                       )
    return es


def drop_index():
    es.indices.delete(index=index_name, ignore=[400, 404])
    print('%s index dropped ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def create_index():
    # print('%s create new index ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    doc = '''
    {  
      "mappings":{  
        "assignment":{  
          "_timestamp":{  
            "enabled":"true"
          },
          "properties":{  
            "@created_at_2":{  
              "type":"date",
              "format":"yyyy-MM-dd HH:mm:ss"
            }
          }
        }
      }
    }'''
    es.indices.create(index=index_name, ignore=400, body=doc)
    print('%s index created ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def get_message():
    esid = 0
    import_list = []
    for message in consumer:
        # time.sleep(1)
        # print("next")
        esid += 1
        msg = json.loads(message.value)
        time_created_at = msg['created_at']
        to_date_type = datetime.strptime(time_created_at, '%a %b %d %H:%M:%S %z %Y')

        msg['@created_at_2'] = datetime.strptime(to_date_type.strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')

        # print(esid, msg)
        import_list.append(msg)
        if esid % MAX_POLL_RECORDS == 0:
            print('%s received in total %d records ' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), esid))
            bulk_insert_to_es(esid, import_list)

            consumer.commit
            print('%s offsets have been committed ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
            import_list.clear()

    bulk_insert_to_es(esid, import_list)
    consumer.commit

    print('%s consumer stopped ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    print('%s offsets have been committed,rest %d rows inserted ' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),len(import_list)))


def bulk_insert_to_es(esid, msg):
    # es.index(index=INDEX_NAME, doc_type= DOC_TYPE, id=esid, body=msg)
    helpers.bulk(es, msg, index=index_name, doc_type=doc_type)
    print('%s %d record inserted ' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), len(msg)))


if __name__ == '__main__':
    es = connect_to_es('es_config')
    drop_index()
    create_index()
    get_message()

