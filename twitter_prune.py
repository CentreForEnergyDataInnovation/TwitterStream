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



while True:

    for root in tweet_tree.find({ "scrape_status" : "Root" }).sort([("_id",1)]).collation(Collation("en_US",numericOrdering=True)).limit(1000):
        if tweet_tree.count_documents({"ancestors" : root["_id"]}) > 1000:
            print(tweet_tree.count_documents({"ancestors" : root["_id"]}))
        for sub in tweet_tree.find({"ancestors" : root["_id"]}).sort([("in_reply_to_status_id_str",1)]):
            c = 1

        #print(root["_id"])
        #print(root["hashtagsInTree"])

    break