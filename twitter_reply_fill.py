from datetime import datetime
import pymongo
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

"""
for x in users_to_search.find().sort("_id", 1).collation(Collation(locale="en_US",numericOrdering=True)):
    results = users_to_search.find({ "user_id_str" : x["user_id_str"] })
    if results.count() > 1:
        print(str(results.count()) + " - " + x["user_id_str"])
"""

count = 0

for x in users_to_search.find({ "created_at_dt" : { "$exists" : False } }):
    count += 1
    tweet = tweet_tree.find({ "_id" : x["_id"] })
    users_to_search.update_one(
        { "_id" : x["_id"] },
        {
            "$set" : {
                "created_at" : tweet[0]["created_at"],
                "created_at_dt" : tweet[0]["created_at_dt"],
            }
        }
    )
    if count % 100 == 0:
        print(count)

print("done")