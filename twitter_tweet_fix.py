from datetime import datetime, timezone, timedelta
import time
import pymongo
import bson
from pymongo.collation import Collation
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
poll_seed = twitter_db["pollSeed"]
pollreply_seed = twitter_db["pollReplySeed"]

tweets_staging = twitter_db["tweets_staging"]

count = tweets.count_documents({ "id_str" : { "$exists" : False }, "collect" : { "$ne" : True } })

while count > 0:

    

    for tweet in tweets.find({ "id_str" : { "$exists" : False }, "collect" : { "$ne" : True } }).limit(1000):
        count -= 1
        tweets_to_collect.replace_one({"_id": tweet["_id"]}, {"_id": tweet["_id"]}, True)
        tweets.update_one(
            { "_id" : tweet["_id"] },
            {
                "$set" : {
                    "collect" : True
                }
            }
        )
        print(count)

    

    if count == 0:
        count = tweets.count_documents({ "id_str" : { "$exists" : False }, "collect" : { "$ne" : True } })

print("done")

count = tweets.count_documents({ "truncated" : True, "collect" : { "$ne" : True } })

while count > 0:

    for tweet in tweets.find({ "truncated" : True, "collect" : { "$ne" : True } }).limit(1000):
        count -= 1
        tweets_to_collect.replace_one({"_id": tweet["_id"]}, {"_id": tweet["_id"]}, True)
        tweets.update_one(
            { "_id" : tweet["_id"] },
            {
                "$set" : {
                    "collect" : True
                }
            }
        )
        print(count)

    

    if count == 0:
        count = tweets.count_documents({ "truncated" : True, "collect" : { "$ne" : True } })

print("done")