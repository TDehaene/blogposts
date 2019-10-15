# coding: utf-8
"""Collects tweets using hashtags #"""
import datetime
import json
import time
import urllib3
import sys

import tweepy
from google.cloud import pubsub_v1
from tweepy.streaming import StreamListener


# Configuration
PROJECT = ''
PUBSUB_TOPIC = ''

# Twitter authentication
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN = ''
ACCESS_TOKEN_SECRET = ''

# Define the list of terms to listen to
_HASHTAGS = ["#got", "#gameofthrones"]

# Authenticate to the API
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth,
                 wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, PUBSUB_TOPIC)


# Method to push messages to pubsub
def write_to_pubsub(data):
    try:
        if data['lang'] == 'en':
            publisher.publish(topic_path, data=json.dumps({
                'text': data['text'],
                'user_id': data['user_id'],
                'id': data['id'],
                'posted_at': datetime.datetime.fromtimestamp(
                    data['created_at']).strftime('%Y-%m-%d %H:%M:%S')
            }).encode('utf-8'), tweet_id=str(data['id']).encode('utf-8'))
    except Exception as e:
        raise


# Method to format a tweet from tweepy
def reformat_tweet(tweet):
    x = tweet

    processed_doc = {
        'id': x['id'],
        'lang': x['lang'],
        'retweeted_id': x['retweeted_status'][
            'id'] if 'retweeted_status' in x else None,
        'favorite_count': x['favorite_count'] if 'favorite_count' in x else 0,
        'retweet_count': x['retweet_count'] if 'retweet_count' in x else 0,
        'coordinates_latitude': x['coordinates']['coordinates'][0] if x[
            'coordinates'] else 0,
        'coordinates_longitude': x['coordinates']['coordinates'][0] if x[
            'coordinates'] else 0,
        'place': x['place']['country_code'] if x['place'] else None,
        'user_id': x['user']['id'],
        'created_at': time.mktime(
            time.strptime(x['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
    }

    if x['entities']['hashtags']:
        processed_doc['hashtags'] = [
            {'text': y['text'], 'startindex': y['indices'][0]} for y in
            x['entities']['hashtags']]
    else:
        processed_doc['hashtags'] = []

    if x['entities']['user_mentions']:
        processed_doc['usermentions'] = [
            {'screen_name': y['screen_name'], 'startindex': y['indices'][0]} for
            y in
            x['entities']['user_mentions']]
    else:
        processed_doc['usermentions'] = []

    if 'extended_tweet' in x:
        processed_doc['text'] = x['extended_tweet']['full_text']
    elif 'full_text' in x:
        processed_doc['text'] = x['full_text']
    else:
        processed_doc['text'] = x['text']

    return processed_doc


# Custom listener class
class Listener(StreamListener):
    ''' A listener handles tweets that are received from the stream.
    This is a basic listener that just pushes tweets to Google Cloud PubSub
    '''
    def __init__(self):
        super(Listener, self).__init__()
        self._counter = 0

    def on_status(self, status):
        write_to_pubsub(reformat_tweet(status._json))
        self._counter += 1
        print(status._json)
        return True

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        if status_code == 420:
            print('Rate limit active')
            return False


def start_stream(stream, **kwargs):
    try:
        stream.filter(**kwargs)
    except urllib3.exceptions.ReadTimeoutError:
        stream.disconnect()
        print('ReadTimeoutError exception')
        start_stream(stream, **kwargs)
    except Exception as e:
        stream.disconnect()
        print('Fatal exception. Consult logs.', e)
        start_stream(stream, **kwargs)


listener = Listener()
stream = tweepy.Stream(auth, listener=listener, tweet_mode='extended')

try:
    print('Start streaming.')
    start_stream(stream, track=_HASHTAGS)
except KeyboardInterrupt:
    print('Stopped.')
finally:
    print('Done.')
    stream.disconnect()
