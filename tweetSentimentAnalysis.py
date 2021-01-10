import pandas as pd


if __name__ == '__main__':
    df = pd.read_csv("Tweets.csv")
    file1 = open('positive.txt', 'r')
    positives = file1.read().splitlines()
    file1.close()
    file1 = open('negative.txt', 'r')
    negatives = file1.read().splitlines()
    file1.close()
    del df['_id']
    del df['time']
    del df['location']
    del df['user']
    tweets = []
    for x in df['text']:
        words = x.split()
        positive = 0
        negative = 0
        for y in words:
            positive += positives.count(y)
            negative += negatives.count(y)
        if positive == negative:
            sentiment = 'neutral'
        elif positive > negative:
            sentiment = 'positive'
        elif positive < negative:
            sentiment = 'negative'
        tweet = {
            'text': x,
            'sentiment': sentiment
        }
        tweets.append(tweet)
    df = pd.DataFrame(tweets)
    print(df)
    df.to_csv("tweetSentiments.csv", encoding='utf-8')
