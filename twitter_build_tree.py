from datetime import datetime, timezone, timedelta
import time
import pymongo
from pymongo.collation import Collation
import pytz

from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterPager

from twitter_api_constants import *

from process_tweet import process_tweet

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

print("Constructing Tree")
"""
for item in tweet_tree.find().sort("_id", 1).collation(Collation(locale="en_US",numericOrdering=True)):
    print(item)
    tweet_id = item["_id"]
    reply_to_id = item["in_reply_to_status_id"]

    if reply_to_id is None:
        print(tweet_id)
"""

countnum = 0

while True:
    tweetchange = tweet_tree.find_one({ "in_reply_to_status_id_str" : { "$exists" : False }} )

    if tweetchange is None:
        break

    countnum += 1
    tweet_id = tweetchange["_id"]
    tweet = tweets.find_one({ "_id" : tweet_id })
    in_reply_to_status_id_str = tweet["in_reply_to_status_id_str"]
    tweet_tree.update_one(
        { "_id" : tweet_id },
        {
            "$set" : {
                "in_reply_to_status_id_str" : in_reply_to_status_id_str
            }
        }
    )
    print(countnum)