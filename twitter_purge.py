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
purge_seed = twitter_db["purgeSeed"]

tweets_staging = twitter_db["tweets_staging"]

offload_tweets = twitter_db["offload_tweets"]
offload_tree = twitter_db["offload_tree"]

valid_hashtags = set()
for x in trackers_hashtags.find():
    if "lowerTerm" in x:
        valid_hashtags.add(x["lowerTerm"])
    else:
        valid_hashtags.add(x["term"].replace("#","").lower())

valid_users = set()
for x in trackers_users.find({ "id_str" : { "$exists" : True } }):
    valid_users.add(x["id_str"])

statusCheckNum = 0

"""
while True:

    tweet_check = offload_tree.find_one({})
    if tweet_check is None:
        break

    for tweet in offload_tree.find({}).limit(1000):
        print("tree " + tweet["_id"])
        tweets_to_collect.replace_one({"_id": tweet["_id"]}, {"_id": tweet["_id"]}, True)
        offload_tree.delete_one({"_id": tweet["_id"]})

while True:

    tweet_check = offload_tweets.find_one({})
    if tweet_check is None:
        break

    for tweet in offload_tweets.find({}).limit(1000):
        print("tweets " + tweet["_id"])
        offload_tweets.delete_one({"_id": tweet["_id"]})

"""
while True:
    purgeCheck = purge_seed.find_one()
    if purgeCheck == None:
        purge_seed.insert_one({"statusCheck": str(statusCheckNum)})
    else:
        oldStatusCheckNum = int(purgeCheck["statusCheck"])
        statusCheckNum = (oldStatusCheckNum + 1) % 5
        purge_seed.replace_one({"statusCheck": str(oldStatusCheckNum)}, {
                            "statusCheck": str(statusCheckNum)})
        print(statusCheckNum)

    count = 0
    valid_count = 0
    tracked_count = 0
    parent_count = 0
    offload_count = 0
    expire_count = 0

    while True:

        tweetCheck = tweet_tree.find_one(
            {
                "cleanCheck" : { "$exists" : True },
                "purgeLoop" : { "$nin" : [str(statusCheckNum), "Expired", "Tracked", "Parent"] }
            }
        )
        if tweetCheck is None:
            print("cycle")
            break

        for tweet in tweet_tree.find(
            {
                "cleanCheck" : { "$exists" : True },
                "purgeLoop" : { "$nin" : [str(statusCheckNum), "Expired", "Tracked", "Parent"] }
            }
        ).sort("_id", -1).collation(Collation("en_US",numericOrdering=True)).limit(1000):
            
            count += 1

            tweet_id = tweet["_id"]
            user_id = tweet["user_id_str"]
            cleanCheck = tweet["cleanCheck"]
            hashtags_in_children = set(tweet["hashtagsInChildren"]) if "hashtagsInChildren" in tweet else set()
            users_in_children = set(tweet["usersInChildren"]) if "usersInChildren" in tweet else set()
            created_at_dt = tweet["created_at_dt"]

            if cleanCheck == True:
                if user_id in valid_users:
                    tracked_count += 1
                    users_to_search.update_one(
                        { "_id" : tweet_id },
                        {
                            "$set" : {
                                "reply_search_status" : "Tracked"
                            }
                        }
                    )
                    tweet_tree.update_one(
                        { "_id" : tweet_id },
                        {
                            "$set" : {
                                "purgeLoop" : "Tracked"
                            }
                        }
                    )
                    print(str(count) + " : valid " + str(valid_count) + " : tracked " + str(tracked_count) + " : parent " + str(parent_count) + " : expired " + str(expire_count) + " : offload " + str(offload_count) + " : " + "tracked" + " : " + tweet_id + " : " + str(created_at_dt))
                    continue

                a = pytz.utc.localize(created_at_dt)
                b = datetime.now(timezone.utc)
                timediff = b - a

                if timediff.days > 9:
                    expire_count += 1
                    users_to_search.update_one(
                        { "_id" : tweet_id },
                        {
                            "$set" : {
                                "reply_search_status" : "Expired"
                            }
                        }
                    )
                    tweet_tree.update_one(
                        { "_id" : tweet_id },
                        {
                            "$set" : {
                                "purgeLoop" : "Expired"
                            }
                        }
                    )
                    print(str(count) + " : valid " + str(valid_count) + " : tracked " + str(tracked_count) + " : parent " + str(parent_count) + " : expired " + str(expire_count) + " : offload " + str(offload_count) + " : " + "expire " + " : " + tweet_id + " : " + str(created_at_dt))
                    continue
                else:
                    valid_count += 1
                    tweet_tree.update_one(
                        { "_id" : tweet_id },
                        {
                            "$set" : {
                                "purgeLoop" : str(statusCheckNum)
                            }
                        }
                    )
                    print(str(count) + " : valid " + str(valid_count) + " : tracked " + str(tracked_count) + " : parent " + str(parent_count) + " : expired " + str(expire_count) + " : offload " + str(offload_count) + " : " + "valid  " + " : " + tweet_id + " : " + str(created_at_dt))
                    continue
            else:
                matching_hashtags = valid_hashtags & hashtags_in_children
                matching_users = valid_users & users_in_children

                if (len(matching_hashtags) + len(matching_users)) > 0:
                    parent_count += 1
                    tweet_tree.update_one(
                        { "_id" : tweet_id },
                        {
                            "$set" : {
                                "purgeLoop" : "Parent"
                            }
                        }
                    )
                    print(str(count) + " : valid " + str(valid_count) + " : tracked " + str(tracked_count) + " : parent " + str(parent_count) + " : expired " + str(expire_count) + " : offload " + str(offload_count) + " : " + "parent " + " : " + tweet_id + " : " + str(created_at_dt))
                    continue
                else:
                    if "quoted_status_id_str" in tweet and tweet["quoted_status_id_str"] is not None:
                        quote_tweet = tweet_tree.find_one({"_id" : tweet["quoted_status_id_str"]})
                        if quote_tweet is None:
                            tweet_tree.update_one(
                                { "_id" : tweet_id },
                                {
                                    "$set" : {
                                        "purgeLoop" : str(statusCheckNum)
                                    }
                                }
                            )
                            tweets_to_collect.replace_one({"_id": tweet["quoted_status_id_str"]}, {"_id": tweet["quoted_status_id_str"]}, True)
                            print("quote not loaded")
                            continue
                        else:
                            if "my_hashtags" in quote_tweet:
                                quote_hashtags = quote_tweet["my_hashtags"]
                                quote_users = quote_tweet["my_users"]

                                matching_hashtags = valid_hashtags & hashtags_in_children
                                matching_users = valid_users & users_in_children

                                if (len(matching_hashtags) + len(matching_users)) > 0:
                                    tweet_tree.update_one(
                                        { "_id" : tweet_id },
                                        {
                                            "$unset" : {
                                                "cleanCheck" : ""
                                            }
                                        }
                                    )
                                    print(tweet_id + " quote not included in tree info")
                                    continue
                                
                            else:
                                tweet_tree.update_one(
                                    { "_id" : tweet_id },
                                    {
                                        "$unset" : {
                                            "cleanCheck" : ""
                                        }
                                    }
                                )
                                print(tweet_id + " quote not included in tree info")
                                continue


                    offload_count += 1
                    offloading_tweet = tweets.find_one({ "_id" : tweet_id })
                    offloading_node = tweet_tree.find_one({ "_id" : tweet_id })
                    
                    if offloading_tweet is not None:
                        offload_tweets.replace_one(
                            { "_id" : tweet_id },
                            offloading_tweet,
                            True
                        )
                    if offloading_node is not None:
                        offload_tree.replace_one(
                            { "_id" : tweet_id },
                            offloading_node,
                            True
                        )

                    tweets.delete_one(
                        { "_id" : tweet_id }
                    )
                    tweet_tree.delete_one(
                        { "_id" : tweet_id }
                    )
                    users_to_search.delete_one(
                        { "_id" : tweet_id }
                    )

                    print(str(count) + " : valid " + str(valid_count) + " : tracked " + str(tracked_count) + " : parent " + str(parent_count) + " : expired " + str(expire_count) + " : offload " + str(offload_count) + " : " + "offload" + " : " + tweet_id + " : " + str(created_at_dt))
                    continue