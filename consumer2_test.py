
import sys
import logging
import json
from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob
import pycountry

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = 'otravo_twitter_stream'
TOPICS_OUTPUT = 'otravo_twitter_stream_output'
MAX_POLL_RECORDS = 500
CONSUMER_TIMEOUT_MS = 60 * 60 * 1000  # stop if no message after 1 hr

tweets_return = []

consumer = KafkaConsumer(TOPICS,
                         bootstrap_servers=BOOTSTRAP_SERVERS,
                         enable_auto_commit='False',
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=CONSUMER_TIMEOUT_MS)

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         acks='all',
                         retries=sys.maxsize,
                         max_in_flight_requests_per_connection=1,
                         linger_ms=5,
                         compression_type='gzip',
                         value_serializer=lambda m: json.dumps(m).encode('ascii')
                         )


class TwitterProcess(object):

    @staticmethod
    def get_tweet_sentiment(tweet):
        # create TextBlob object of passed tweet text
        analysis = TextBlob(tweet)
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

    def get_tweets(self):

        try:

            count = 0
            for message in consumer:
                parsed_tweet = {}
                count += 1
                clean_message = json.loads(message.value)

                # print consumer messages
                # print(message)
                print(count, "  ", clean_message["text"])
                key_words_country = []

                # TODO add a new column 'key_words: <country>' used to determine which country the record is about
                # TODO need to improve
                for country in pycountry.countries:
                    if country.name in clean_message["text"]:
                        print(country.name)
                        key_words_country = country.name
                    elif country.name.upper() in clean_message["text"]:
                        print(country.name)
                        key_words_country = country.name
                    elif country.name.lower() in clean_message["text"]:
                        print(country.name)
                        key_words_country = country.name

                parsed_tweet['id'] = clean_message["id"]
                parsed_tweet['created_at'] = clean_message["created_at"]
                parsed_tweet['text'] = clean_message["text"]
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(clean_message["text"])
                parsed_tweet['key_words'] = key_words_country
                parsed_tweet['source'] = clean_message["source"]

                tweets_return.append(parsed_tweet)

                # send messages
                producer.send(TOPICS_OUTPUT, value=parsed_tweet, key=bytes(message.partition))

                # manual commit
                if count % MAX_POLL_RECORDS == 0:
                    logging.warning("received %d records" % MAX_POLL_RECORDS)
                    consumer.commit
                    logging.warning('offsets have been committed')

            consumer.commit
            logging.warning('Consumer stopped')
            logging.warning('offsets have been committed')
            # print("the final length of tweets is ", len(tweets))
            return tweets_return
        except Exception as e:
            print("Error : " + str(e))


def main():

    process_tweets = TwitterProcess()
    process_tweets.get_tweets()

    # test usages
    # tweets = process_tweets.get_tweets()
    # positive_tweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
    # percentage of positive tweets
    # print(len(positive_tweets))
    # print("Positive tweets percentage: {} %".format(100 * len(positive_tweets) / len(tweets)))
    # picking negative tweets from tweets
    # negative_tweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
    # percentage of negative tweets
    # print("Negative tweets percentage: {} %".format(100 * len(negative_tweets) / len(tweets)))
    # percentage of neutral tweets
    # print("Neutral tweets percentage: {} % ".
    #       format(100 * ((len(tweets) - len(negative_tweets) - len(positive_tweets)) / len(tweets))))


if __name__ == "__main__":
        # calling main function
        main()
