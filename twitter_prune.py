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

valid_hashtags = set()
for x in trackers_hashtags.find():
    if "lowerTerm" in x:
        valid_hashtags.add(x["lowerTerm"])
    else:
        valid_hashtags.add(x["term"].replace("#","").lower())

valid_users = set()
for x in trackers_users.find({ "id_str" : { "$exists" : True } }):
    valid_users.add(x["id_str"])

count = 0
subcount = 0
valid_count = 0
invalid_count = 0

tweet_tree.update_many({},{"$unset" : {"cleanCheck":""}})

while True:

    if tweet_tree.count_documents({"cleanCheck":{"$exists" : False}, "scrape_status" : "Root" }) == 0:
        break

    for root in tweet_tree.find({"cleanCheck":{"$exists" : False}, "scrape_status" : "Root" }).sort([("_id",1)]).collation(Collation("en_US",numericOrdering=True)).limit(1000):
        count += 1
        subcount = 0
        hashtags = set()
        users = set()

        tweet_ids = set()

        tweet_ids.add(root["_id"])
        subcount += 1

        if root["entities"] is not None:
            for x in root["entities"]["hashtags"]:
                hashtags.add(x["text"].lower())

            for x in root["entities"]["user_mentions"]:
                users.add(x["id_str"])
                
            
            
        if "quoted_status_id_str" in root:
            quote = tweet_tree.find_one({"_id":root["quoted_status_id_str"]})

            if quote is not None and quote["entities"] is not None:
                for x in quote["entities"]["hashtags"]:
                    hashtags.add(x["text"].lower())

                for x in quote["entities"]["user_mentions"]:
                    users.add(x["id_str"])

        for sub in tweet_tree.find({"ancestors" : root["_id"]}).sort([("in_reply_to_status_id_str",1)]):
            subcount += 1

            tweet_ids.add(sub["_id"])

            for x in sub["entities"]["hashtags"]:
                hashtags.add(x["text"].lower())

            for x in sub["entities"]["user_mentions"]:
                users.add(x["id_str"])

            if "quoted_status_id_str" in sub:
                quote = tweet_tree.find_one({"_id":sub["quoted_status_id_str"]})
                
                if quote is not None and quote["entities"] is not None:
                    for x in quote["entities"]["hashtags"]:
                        hashtags.add(x["text"].lower())

                    for x in quote["entities"]["user_mentions"]:
                        users.add(x["id_str"])

        #print(root["_id"])
        #print(root["hashtagsInTree"])

        if (len(valid_hashtags & hashtags) + len(valid_users & users)) > 0:
            valid_count += subcount

            if len(valid_hashtags & hashtags) > 0 and len(valid_users & users) > 0:
                print(str(count) + " | " + str(valid_count) + " | " + str(invalid_count) + " - " + root["_id"] + " " + str(valid_hashtags & hashtags) + " " + str(valid_users & users))
            elif len(valid_hashtags & hashtags) > 0:
                print(str(count) + " | " + str(valid_count) + " | " + str(invalid_count) + " - " + root["_id"] + " " + str(valid_hashtags & hashtags))
            elif len(valid_users & hashtags) > 0:
                print(str(count) + " | " + str(valid_count) + " | " + str(invalid_count) + " - " + root["_id"] + " " + str(valid_users & users))

            tweet_tree.update_many(
                { 
                    "_id" : {
                        "$in" : list(tweet_ids) 
                    } 
                },
                {
                    "$set" : {
                        "cleanCheck" : True
                    }
                }
            )

            users_to_search.update_many(
                { 
                    "_id" : {
                        "$in" : list(tweet_ids) 
                    } 
                },
                {
                    "$set" : {
                        "valid" : True
                    }
                }
            )

        else:
            invalid_count += subcount

            print(str(count) + " | " + str(valid_count) + " | " + str(invalid_count) + " - " + root["_id"])

            tweet_tree.update_many(
                { 
                    "_id" : {
                        "$in" : list(tweet_ids) 
                    } 
                },
                {
                    "$set" : {
                        "cleanCheck" : False
                    }
                }
            )
            users_to_search.update_many(
                { 
                    "_id" : {
                        "$in" : list(tweet_ids) 
                    } 
                },
                {
                    "$set" : {
                        "valid" : False
                    }
                }
            )