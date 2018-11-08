import sys
import logging
import json
from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob


BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = 'country_twitter_stream2'
TOPICS_OUTPUT = 'country_twitter_stream_output'
MAX_POLL_RECORDS = 10
tweets_return = []

# consumer = KafkaConsumer(TOPICS, bootstrap_servers=BOOTSTRAP_SERVERS, enable_auto_commit='False', max_poll_records=10)
consumer = KafkaConsumer(TOPICS, bootstrap_servers=BOOTSTRAP_SERVERS, auto_offset_reset='earliest',
                         consumer_timeout_ms=30000)
# Kafka Configuration
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

                # print(clean_message)
                print(count, "  ", clean_message["text"], "\n")

                parsed_tweet['text'] = clean_message["text"]
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(clean_message["text"])
                tweets_return.append(parsed_tweet)

                # make sure messages still go to the same partition
                producer.send(TOPICS_OUTPUT, value=parsed_tweet, key=bytes(message.partition))

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
    tweets = process_tweets.get_tweets()

    # print(tweets[0])
    # picking positive tweets from tweets
    # positive_tweets = []
    # for tweet in tweets:
    #     if tweet['sentiment'] == 'positive':
    #         positive_tweets.append(tweet)

    # positive_tweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
    # # percentage of positive tweets
    # # print(len(positive_tweets))
    # print("Positive tweets percentage: {} %".format(100 * len(positive_tweets) / len(tweets)))
    # # picking negative tweets from tweets
    # negative_tweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
    # # percentage of negative tweets
    # print("Negative tweets percentage: {} %".format(100 * len(negative_tweets) / len(tweets)))
    # # percentage of neutral tweets
    # print("Neutral tweets percentage: {} % ".
    #       format(100 * ((len(tweets) - len(negative_tweets) - len(positive_tweets)) / len(tweets))))


if __name__ == "__main__":
        # calling main function
        main()
