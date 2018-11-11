"""
Created on 11/09/2018
@author: Ren Zhe
"""
import sys
import tweepy
from kafka import KafkaProducer
import readconfig


WORDS = ['Spain', '#Spain', 'Turkey', '#Turkey', 'Italy', '#Italy']
# PARTITION = "Asia"
kafka_cfg = readconfig.read_config('kafka_config')
topics = kafka_cfg['topics']
# topics = "country_twitter_stream2"
bootstrap_servers = kafka_cfg['bootstrap_servers']


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
            producer.send(topics, data.encode('utf-8'))
            # producer.send(TOPICS, data.encode('utf-8'), PARTITION.encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

    # @staticmethod
    # def __read_config():
    #     configs = {}
    #     cf = configparser.ConfigParser()
    #     cf.read(CONFIG_FILE)
    #     configs['token'] = cf.get('info', 'token')
    #     configs['consumer_key'] = cf.get('info', 'consumer_key')
    #     configs['consumer_secret'] = cf.get('info', 'consumer_secret')
    #     configs['token_secret'] = cf.get('info', 'token_secret')
    #     return configs


def producer_config():

    producer_cfg = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                 acks='all',
                                 retries=sys.maxsize,
                                 max_in_flight_requests_per_connection=1,
                                 linger_ms=5,
                                 compression_type='gzip')
    return producer_cfg


def twitter_api_config():

    api_config = readconfig.read_config('auth_info')
    consumer_key = api_config['consumer_key']
    consumer_secret = api_config['consumer_secret']
    access_key = api_config['token']
    access_secret = api_config['token_secret']

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    return auth


if __name__ == '__main__':
    # Kafka Configuration
    producer = producer_config()
    tweepy_auth = twitter_api_config()
    api = tweepy.API(tweepy_auth)

    # Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
    listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=30,
                                             retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
    stream = tweepy.Stream(auth=tweepy_auth, listener=listener)

    print("Tracking: " + str(WORDS))

    stream.filter(track=WORDS, languages=['en'])
