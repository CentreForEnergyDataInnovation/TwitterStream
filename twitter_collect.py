from datetime import datetime, timezone, timedelta
import time
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

while True:

    tweetsToCollect = []
    for item in tweets_to_collect.find().sort("_id", 1).collation(Collation(locale="en_US", numericOrdering=True)).limit(100):
        tweetsToCollect.append(item["_id"])
        tweets_to_collect.delete_one({"_id" : item["_id"]})

    if len(tweetsToCollect) == 0:
        time.sleep(5)
        continue

    tweetsToSearch = (",").join(tweetsToCollect)
    print("reply lookup: " +tweetsToSearch)
    r = api.request("statuses/lookup", { "id" : tweetsToSearch, "tweet_mode" : "extended", "map" : True})

    for item in r:
        for subitem in item["id"]:
            if item["id"][subitem] is not None:
                process_tweet(item["id"][subitem], users, users_to_search, tweets, tweet_tree, tweets_to_collect)
            else:
                if tweets.find({"_id":subitem,"id_str" : {"$exists" : False}}) is not None:
                    tweets.update_one(
                        {"_id":subitem},
                        {
                            "$set" : {
                                "id_str" : subitem,
                                "tweet_text" : "This Tweet is unavailable."
                            }
                        }
                    )
                if tweet_tree.find_one({"_id":subitem}) is None:
                    tweet_tree.insert_one(
                        {
                            "_id" : subitem,
                            "tweet_text" : "This Tweet is unavailable.",
                            "user_id_str" : None,
                            "in_reply_to_status_id_str" : None,
                            "quoted_status_id_str" : None,
                            "created_at" : None,
                            "created_at_dt" : None,
                            "entities" : None,
                            "quote_count" : 0,
                            "reply_count" : 0,
                            "retweet_count" : 0,
                            "favorite_count" : 0,
                            "ancestors" : [],
                            "scrape_status": "Root"
                        }
                    )

    time.sleep(5)