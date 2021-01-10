from pymongo import MongoClient
import re

RawDbClient = MongoClient('', 27017) # Add URL

ProcessedDbClient = MongoClient('', 27017) # Add URL

RawDb = RawDbClient.RawDb
RawTweets = RawDb.tweets

ProcessedDb = ProcessedDbClient.ProcessedDb
ProcessedTweets = ProcessedDb.tweets

# Keywords and language
keywords = ['Storm', 'Winter', 'Canada', 'Temperature', 'Flu', 'Snow', 'Indoor', 'Safety']
language = ['en']


def cleanandstore(RawTweets, ProcessedTweets):

    for tweet in RawTweets.find():
        text = re.sub(r'htt\S+', '', tweet['text'], flags=re.MULTILINE)
        text = re.compile('[\U00010000-\U0010ffff]', flags=re.MULTILINE).sub(r'', text)
        text = re.sub(r"[^a-zA-Z0-9]+", ' ', text)

        if tweet['place'] is not None:
            location = tweet['place']['name']
        else:
            location = None
        tweetObj = {
            'text': text,
            'time': tweet['created_at'],
            'location': location,
            'user': tweet['user']['screen_name']
        }

        ProcessedTweets.insert_one(tweetObj)

    return True


if __name__ == '__main__':
   cleanandstore(RawTweets, ProcessedTweets)
