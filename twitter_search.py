from datetime import datetime
import pymongo
import pytz

from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterPager

from twitter_api_constants import *

from process_tweet import process_tweet

# Set up Twitter API

api = TwitterAPI(
    CONSUMER_KEY,
    CONSUMER_SECRET,
    auth_type="oAuth2"
)

# Set up Mongo

MONGO = pymongo.MongoClient(MONGO_CONNECTION_STRING)
twitter_db = MONGO["twitters"]
trackers_hashtags = twitter_db["trackersHashtags"]
trackers_users = twitter_db["trackersUsers"]
update_settings = twitter_db["updated"]

tweets = twitter_db["tweets"]
users = twitter_db["users"]
users_to_search = twitter_db["usersToSearch"]
tweets_to_collect = twitter_db["tweetsToCollect"]
tweet_tree = twitter_db["tweetTree"]

tweet_count = 0
retweet_count = 0
total_count = 0

r = TwitterPager(api, "search/tweets", { "q" : SEARCH_TERM, "count" : SEARCH_COUNT, "result_type" : "recent" })

for item in r.get_iterator():
    total_count += 1
    if "retweeted_status" in item:
        retweet_count += 1
    else:
        tweet_count += 1
        process_tweet(item, users, users_to_search, tweets, tweet_tree)
    
    if "message" in item and item["code"] == 88:
        print("RATELIMIT")
        print(item["message"])
        break

    print(str(total_count) + " : " + str(tweet_count) + " : " + str(retweet_count))

print("Done")