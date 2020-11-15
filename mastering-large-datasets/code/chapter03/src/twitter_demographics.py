# Tweet-Level Pipeline
from toolz import pipe
import twitter

# Authenticate our application
Twitter = twitter.Api(consumer_key="",
                      consumer_secret="",
                      access_token_key="",
                      access_token_secret="")

# Look up an individual tweet
def get_tweet_from_id(tweet_id, api=Twitter):
    return api.GetStatus(tweet_id, trim_user=True)

# Gets text from tweet
def tweet_to_text(tweet)
    return tweet.text

# Splits text into tokens
def tokenize_text(text):
    return text.split()

# Scores text based on our lexicon
def score_text(tokens):
    lexicon = {"the":1, "to":1, "and":1,
             "in":1, "have":1, "it":1,
             "be":-1, "of":-1, "a":-1,
             "that":-1, "i":-1, "for":-1}
    return sum(map(lambda x: lexicon.get(x, 0), tokens))

# Pipes a tweet through our pipeline
def score_tweet(tweet_id):
    return pipe(tweet_id, get_tweet_from_id, tweet_to_text,
                          tokenize_text, score_text)



# User-Level Pipeline
from toolz import compose

def score_user(tweets):
    N = len(tweets)
    total = sum(map(score_tweet, tweets))
    return total / N

def categorize_user(user_score):
    if user_score > 0:
        return {"score":user_score,
                "gender": "Male"}
    return {"score":user_score,
            "gender":"Female"}

pipeline = compose(categorize_user, score_user)



# Applying the Pipeline
users_tweets = [
[1056365937547534341, 1056310126255034368, 1055985345341251584,
 1056585873989394432, 1056585871623966720],
[1055986452612419584, 1056318330037002240, 1055957256162942977,
 1056585921154420736, 1056585896898805766],
[1056240773572771841, 1056184836900175874, 1056367465477951490,
 1056585972765224960, 1056585968155684864],
[1056452187897786368, 1056314736546115584, 1055172336062816258,
 1056585983175602176, 1056585980881207297]]

with Pool() as P:
    print(P.map(pipeline, users_tweets))


user_tweets = [
        ["i think product x is so great", "i use product x for everything",
        "i couldn't be happier with product x"],
        ["i have to throw product x in the trash",
        "product x... the worst value for your money"],
        ["product x is mostly fine", "i have no opinion of product x"]]

with Pool() as P:
    print(P.map(gender_prediction_pipeline, users_tweets))