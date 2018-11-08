"""
Created on 11/09/2018
@author: Ren Zhe
"""
import sys
import tweepy
import configparser
from kafka import KafkaProducer

CONFIG_FILE = './config.cfg'
WORDS = ['China', '#China']
PARTITION = "Asia"
TOPICS = "country_twitter_stream2"
BOOTSTRAP_SERVERS = 'localhost:9092'


class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print("Error received in kafka producer " + repr(status_code))
        return True # Don't kill the stream

    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        try:
            # producer.send(TOPICS, data.encode('utf-8'))
            producer.send(TOPICS, data.encode('utf-8'), PARTITION.encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

    @staticmethod
    def __read_config():
        configs = {}
        cf = configparser.ConfigParser()
        cf.read(CONFIG_FILE)
        configs['token'] = cf.get('info', 'token')
        configs['consumer_key'] = cf.get('info', 'consumer_key')
        configs['consumer_secret'] = cf.get('info', 'consumer_secret')
        configs['token_secret'] = cf.get('info', 'token_secret')
        return configs


# @staticmethod
def read_config():
        configs = {}
        cf = configparser.ConfigParser()
        cf.read(CONFIG_FILE)
        configs['token'] = cf.get('info', 'token')
        configs['consumer_key'] = cf.get('info', 'consumer_key')
        configs['consumer_secret'] = cf.get('info', 'consumer_secret')
        configs['token_secret'] = cf.get('info', 'token_secret')
        return configs


# Kafka Configuration
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         acks='all',
                         retries=sys.maxsize,
                         max_in_flight_requests_per_connection=1,
                         linger_ms=5,
                         compression_type='gzip'
                         )


api_config = read_config()
consumer_key = api_config['consumer_key']
consumer_secret = api_config['consumer_secret']
access_key = api_config['token']
access_secret = api_config['token_secret']

# Create Auth object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

api = tweepy.API(auth)

# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=30,
                                         retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
stream = tweepy.Stream(auth=auth, listener=listener)

print("Tracking: " + str(WORDS))

stream.filter(track=WORDS, languages=['en'])
# stream.filter(track=[WORDS])
