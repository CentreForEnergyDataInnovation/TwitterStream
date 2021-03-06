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
valid_count = 0
invalid_count = 0
while True:

    for tweet in tweet_tree.find(
        {
            "$or" : [
                { "cleanCheck" : { "$exists" : False } },
                { "my_hashtags" : { "$exists" : False } }
            ],
            "scrape_status" : { "$in" : ["Root", "Linked"] }
        }
    ).sort("_id", 1).collation(Collation("en_US",numericOrdering=True)).limit(1000):
        tweet_id = tweet["_id"]
        hashtags = set()
        user_mentions = set()

        count += 1

        if tweet["user_id_str"] is not None:
            user_mentions.add(tweet["user_id_str"])
        if tweet["entities"] is not None:
            for x in tweet["entities"]["hashtags"]:
                hashtags.add(x["text"].lower())
            for x in tweet["entities"]["user_mentions"]:
                user_mentions.add(x["id_str"])

        full_tweet = tweets.find_one({ "_id" : tweet_id })
        if full_tweet is not None:
            if "extended_tweet" in full_tweet:
                for x in full_tweet["extended_tweet"]["entities"]["hashtags"]:
                    hashtags.add(x["text"].lower())
                for x in full_tweet["extended_tweet"]["entities"]["user_mentions"]:
                    user_mentions.add(x["id_str"])
            if "entities" in full_tweet:
                for x in full_tweet["entities"]["hashtags"]:
                    hashtags.add(x["text"].lower())
                for x in full_tweet["entities"]["user_mentions"]:
                    user_mentions.add(x["id_str"])

        if "quoted_status_id_str" in tweet and tweet["quoted_status_id_str"] is not None:
            quote = tweet_tree.find_one({"_id":tweet["quoted_status_id_str"]})
            if quote is not None and quote["user_id_str"] is not None:
                user_mentions.add(quote["user_id_str"])            
            if quote is not None and quote["entities"] is not None:
                for x in quote["entities"]["hashtags"]:
                    hashtags.add(x["text"].lower())
                for x in quote["entities"]["user_mentions"]:
                    user_mentions.add(x["id_str"])
            if quote is None:
                print("quote tree null - " + str(tweet["quoted_status_id_str"]))
                tweets_to_collect.replace_one({"_id": tweet["quoted_status_id_str"]}, {"_id": tweet["quoted_status_id_str"]}, True)
                continue

            quote_tweet = tweets.find_one({ "_id" : tweet["quoted_status_id_str"] })
            if quote is None:
                quote_tweet("quote tweet null - " + tweet["quoted_status_id_str"])
                continue
            if quote_tweet is not None:
                if "extended_tweet" in quote_tweet:
                    for x in quote_tweet["extended_tweet"]["entities"]["hashtags"]:
                        hashtags.add(x["text"].lower())
                    for x in quote_tweet["extended_tweet"]["entities"]["user_mentions"]:
                        user_mentions.add(x["id_str"])
                if "entities" in quote_tweet:
                    for x in quote_tweet["entities"]["hashtags"]:
                        hashtags.add(x["text"].lower())
                    for x in quote_tweet["entities"]["user_mentions"]:
                        user_mentions.add(x["id_str"])

        if "ancestors" in tweet:
            ancestors = tweet["ancestors"]
        else:
            ancestors = []
        if len(ancestors) > 0:
            tweet_tree.update_many(
                { "_id" : { "$in" : ancestors } },
                {
                    "$addToSet" : {
                        "hashtagsInChildren" : { "$each" : list(hashtags) },
                        "usersInChildren" : { "$each" : list(user_mentions) }
                    }
                }
            )

        parent_valid = False
        tweet_valid = False

        if tweet["in_reply_to_status_id_str"] is not None:
            parent = tweet_tree.find_one({ "_id" : tweet["in_reply_to_status_id_str"] })
            if parent is not None:
                if "cleanCheck" in parent:
                    parent_valid = parent["cleanCheck"]

        if parent_valid == True:
            tweet_valid = True
        else:
            matching_hashtags = valid_hashtags & hashtags
            matching_users = valid_users & user_mentions
            
            if (len(matching_hashtags) + len(matching_users)) > 0:
                tweet_valid = True

        if tweet_valid == True:

            if "cleanCheck" in tweet and tweet["cleanCheck"] == False:
                print("prior bad parse")

            tweet_tree.update_one(
                { "_id" : tweet_id },
                {
                    "$set" : {
                        "cleanCheck" : True,
                        "my_hashtags" : list(hashtags),
                        "my_users" : list(user_mentions)
                    }
                }
            )
            users_to_search.update_one(
                { "_id" : tweet_id },
                {
                    "$set" : {
                        "valid" : True
                    }
                }
            )
            valid_count += 1

            if parent_valid == True:
                print("V - " + str(count) + " | " + str(valid_count) + " + " + str(invalid_count) + " | " + str(len(ancestors)) + " | " + tweet_id + " - " + "Parent")
            elif len(matching_hashtags) > 0 and len(matching_users) > 0:
                print("V - " + str(count) + " | " + str(valid_count) + " + " + str(invalid_count) + " | " + str(len(ancestors)) + " | " + tweet_id + " - " + str(matching_hashtags) + " " + str(matching_users))
            elif len(matching_hashtags) > 0:
                print("V - " + str(count) + " | " + str(valid_count) + " + " + str(invalid_count) + " | " + str(len(ancestors)) + " | " + tweet_id + " - " + str(matching_hashtags))
            elif len(matching_users) > 0:
                print("V - " + str(count) + " | " + str(valid_count) + " + " + str(invalid_count) + " | " + str(len(ancestors)) + " | " + tweet_id + " - " + str(matching_users))
            else:
                print("V - " + str(count) + " | " + str(valid_count) + " + " + str(invalid_count) + " | " + str(len(ancestors)) + " | " + tweet_id + " - " + "WTF")
        else:
            tweet_tree.update_one(
                { "_id" : tweet_id },
                {
                    "$set" : {
                        "cleanCheck" : False,
                        "my_hashtags" : list(hashtags),
                        "my_users" : list(user_mentions)
                    }
                }
            )
            users_to_search.update_one(
                { "_id" : tweet_id },
                {
                    "$set" : {
                        "valid" : False
                    }
                }
            )
            invalid_count += 1
            print("I - " + str(count) + " | " + str(valid_count) + " + " + str(invalid_count) + " | " + str(len(ancestors)) + " | " + tweet_id + " - " + "Invalid")





"""

while True:

    time.sleep(5)

    count = 0
    subcount = 0
    valid_count = 0
    invalid_count = 0
    valid_root = 0
    invalid_root = 0

    print("Root Parse")

    while True:
        if tweet_tree.count_documents({"cleanCheck":{"$exists" : False}, "scrape_status" : "Root" }) == 0:
            break

        for root in tweet_tree.find({"cleanCheck":{"$exists" : False}, "scrape_status" : "Root" }).limit(1000):
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

            for sub in tweet_tree.find({"ancestors" : root["_id"]}):
                subcount += 1

                tweet_ids.add(sub["_id"])

                if sub["entities"] is not None:
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

            tweet_tree.update_one(
                { "_id" : root["_id"] },
                {
                    "$set" : {
                        "hashtagsInTree" : list(hashtags),
                        "usersInTree" : list(users)
                    }
                }
            )

            if (len(valid_hashtags & hashtags) + len(valid_users & users)) > 0:
                valid_count += subcount
                valid_root += 1

                if len(valid_hashtags & hashtags) > 0 and len(valid_users & users) > 0:
                    print(str(count) + " | " + str(valid_root) + " | " + str(valid_count) + " | " + str(invalid_root) + " | " + str(invalid_count) + " V (" +str(subcount) + ") - " + root["_id"] + " " + str(valid_hashtags & hashtags) + " " + str(valid_users & users))
                elif len(valid_hashtags & hashtags) > 0:
                    print(str(count) + " | " + str(valid_root) + " | " + str(valid_count) + " | " + str(invalid_root) + " | " + str(invalid_count) + " V (" +str(subcount) + ") - " + root["_id"] + " " + str(valid_hashtags & hashtags))
                elif len(valid_users & users) > 0:
                    print(str(count) + " | " + str(valid_root) + " | " + str(valid_count) + " | " + str(invalid_root) + " | " + str(invalid_count) + " V (" +str(subcount) + ") - " + root["_id"] + " " + str(valid_users & users))

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
                invalid_root += 1

                print(str(count) + " | " + str(valid_root) + " | " + str(valid_count) + " | " + str(invalid_root) + " | " + str(invalid_count) + " I (" +str(subcount) + ") - " + root["_id"])

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
    

    print("Linked Parse")
    

    for linked in tweet_tree.find({"cleanCheck":{"$exists" : False}, "scrape_status" : "Linked" },sort=[("_id", -1)], collation = Collation(locale="en_US", numericOrdering=True)).limit(1000):

        if "cleanCheck" in linked:
            print("skip")
            continue

        ancestors = linked["ancestors"]

        minSnowflake = min(ancestors, key=bson.int64.Int64)

"""
"""
        alpha = tweet_tree.find_one(
            {
                "_id" : { "$in" : ancestors },
                "scrape_status" : "Root"
            }
        )
"""
"""
        alpha = tweet_tree.find_one(
            {
                "_id" : minSnowflake,
                "scrape_status" : "Root"
            }
        )

        if alpha is None:
            { "_id" : linked["_id"] },
            {
                "$unset" : {
                    "scrape_status" : ""
                }
            }
            continue

        hashtags = set()
        users = set()

        tweet_ids = set()

        tweet_ids.add(alpha["_id"])

        if alpha["entities"] is not None:
            for x in alpha["entities"]["hashtags"]:
                hashtags.add(x["text"].lower())

            for x in alpha["entities"]["user_mentions"]:
                users.add(x["id_str"])
                
            
            
        if "quoted_status_id_str" in alpha:
            quote = tweet_tree.find_one({"_id":alpha["quoted_status_id_str"]})

            if quote is not None and quote["entities"] is not None:
                for x in quote["entities"]["hashtags"]:
                    hashtags.add(x["text"].lower())

                for x in quote["entities"]["user_mentions"]:
                    users.add(x["id_str"])

        for sub in tweet_tree.find({"ancestors" : alpha["_id"]}):
            tweet_ids.add(sub["_id"])

            if sub["entities"] is not None:
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

        tweet_tree.update_one(
            { "_id" : alpha["_id"] },
            {
                "$set" : {
                    "hashtagsInTree" : list(hashtags),
                    "usersInTree" : list(users)
                }
            }
        )

        if (len(valid_hashtags & hashtags) + len(valid_users & users)) > 0:
            if len(valid_hashtags & hashtags) > 0 and len(valid_users & users) > 0:
                print(linked["_id"] + " " + str(valid_hashtags & hashtags) + " " + str(valid_users & users))
            elif len(valid_hashtags & hashtags) > 0:
                print(linked["_id"] + " " + str(valid_hashtags & hashtags))
            elif len(valid_users & hashtags) > 0:
                print(linked["_id"] + " " + str(valid_users & users))

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

            print(linked["_id"])

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

"""