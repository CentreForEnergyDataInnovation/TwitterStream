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

count = 0

pre_unprocessed = 0
pre_root = 0
pre_linked = 0
pre_processed = 0
pre_total = 0

while True:
    num_unprocessed = tweet_tree.count_documents({"scrape_status" : {"$exists":False}})
    num_root = tweet_tree.count_documents({"scrape_status" : "Root"})
    num_linked = tweet_tree.count_documents({"scrape_status" : "Linked"})
    num_processed = tweet_tree.count_documents({"scrape_status" : {"$in" : ["0", "1", "2", "3", "4"]}})
    num_total = tweet_tree.count_documents({})
    num_collect = tweets_to_collect.count_documents({})

    if count % 25 == 0:
        print("unprocessed" + " : " + "root" + " : " + "linked" + " : " + "processed" + " : " + "total" + " - " + "collect")

    print(str(num_unprocessed)+"("+str((num_unprocessed-pre_unprocessed))+")" + " : " + str(num_root)+"("+str((num_root-pre_root))+")" + " : " + str(num_linked)+"("+str((num_linked-pre_linked))+")" + " : " + str(num_processed)+"("+str((num_processed-pre_processed))+")" + " : " + str(num_total)+"("+str((num_total-pre_total))+")" + " - " + str(num_collect))
    
    pre_unprocessed = num_unprocessed
    pre_root = num_root
    pre_linked = num_linked
    pre_processed = num_processed
    pre_total = num_total

    count += 1
    time.sleep(1)