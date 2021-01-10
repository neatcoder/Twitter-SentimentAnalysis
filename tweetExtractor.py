from pymongo import MongoClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
import json


TweetsClient = MongoClient('',
                          27017)
RetweetClient = MongoClient('',
                          27017)
dbTweets = TweetsClient.RawDb
collectionTweets = dbTweets.tweets

dbRetweets = RetweetClient.RawDb
collectionRetweets = dbRetweets.retweets

# Keywords and language
keywords = ['Storm', 'Winter', 'Canada', 'Temperature', 'Flu', 'Snow', 'Indoor', 'Safety']
language = ['en']

access_token = # Provide access token
access_token_secret = # Provide token secret


def authenticate():
    consumer_key = # Provide consumer key
    consumer_secret = # Provide consumer secret

    return OAuthHandler(consumer_key, consumer_secret)


def store(tweet, collection):

    collection.insert_one(tweet)


class SListener(StreamListener):

    def __init__(self):
        super().__init__()
        self.max = 5000
        self.count = 0

    def on_data(self, data):
        tweet = json.loads(data)
        if self.count == self.max:
            print("max limit breached")
            return False
        else:
            store(tweet, collectionTweets)
            if tweet['retweeted'] is True:
                api = API(auth)
                retweets = api.retweets(tweet['id'])
                for retweet in retweets:
                    store(retweet, collectionRetweets)
            self.count += 1
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    listener = SListener()
    auth = authenticate()
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    stream.filter(track=keywords, languages=language, is_async=True)
