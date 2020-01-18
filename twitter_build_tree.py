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

print("Constructing Tree")

statusCheckNum = 0

cyclecount = 0


while True:

    pollSeedCheck = poll_seed.find_one()
    if pollSeedCheck == None:
        poll_seed.insert_one({"statusCheck": str(statusCheckNum)})
    else:
        oldStatusCheckNum = int(pollSeedCheck["statusCheck"])
        statusCheckNum = (oldStatusCheckNum + 1) % 5
        poll_seed.replace_one({"statusCheck": str(oldStatusCheckNum)}, {
                            "statusCheck": str(statusCheckNum)})
        print(statusCheckNum)

    while True:
        


        if tweet_tree.find_one({"scrape_status": {"$nin": ["Root", "Linked", str(statusCheckNum)]}}) is None:
            cyclecount = 0
            break

        #tweetCheck = tweet_tree.find_one({"scrape_status": {"$nin": ["Root", "Linked", str(statusCheckNum)]}}, sort=[("_id", 1)], collation = Collation(locale="en_US", numericOrdering=True))
        #for tweetCheck in tweet_tree.find({"scrape_status": {"$nin": ["Root", "Linked", str(statusCheckNum)]}}).limit(1000):
        for tweetCheck in tweet_tree.find({"scrape_status": {"$nin": ["Root", "Linked", str(statusCheckNum)]}}, sort=[("_id", 1)], collation = Collation(locale="en_US", numericOrdering=True)).limit(1000):
        
            cyclecount += 1
        
            print(str(cyclecount) + " - tree building: " + tweetCheck["_id"])
            hashtags_in_tree = set()
            users_in_tree = set()

            users_in_tree.add(tweetCheck["user_id_str"])

            for x in tweetCheck["entities"]["hashtags"]:
                hashtags_in_tree.add(x["text"].lower())
            for x in tweetCheck["entities"]["user_mentions"]:
                users_in_tree.add(x["id_str"])

            if tweetCheck["quoted_status_id_str"] is not None:
                quoteTweet = tweet_tree.find_one(
                    {"_id": tweetCheck["quoted_status_id_str"]})
                if quoteTweet is None:
                    tweets_to_collect.replace_one({"_id": tweetCheck["quoted_status_id_str"]}, {
                                                "build_tree": True}, True)
                    tweet_tree.update_one(
                        {"_id": tweetCheck["_id"]},
                        {
                            "$set": {
                                "scrape_status": str(statusCheckNum)
                            }
                        }
                    )
                    continue
                else:
                    if quoteTweet["entities"] is not None:
                        for x in quoteTweet["entities"]["hashtags"]:
                            hashtags_in_tree.add(x["text"].lower())
                        for x in quoteTweet["entities"]["user_mentions"]:
                            users_in_tree.add(x["id_str"])
                        users_in_tree.add(quoteTweet["user_id_str"])

            if tweetCheck["in_reply_to_status_id_str"] is None:
                tweet_tree.update_one(
                    {"_id": tweetCheck["_id"]},
                    {
                        "$set": {
                            "scrape_status": "Root"
                        },
                        "$addToSet": {
                            "hashtagsInTree": { "$each" : list(hashtags_in_tree) },
                            "usersInTree": { "$each" : list(users_in_tree) }
                        }
                    }
                )
            else:
                parentTweet = tweet_tree.find_one(
                    {"_id": tweetCheck["in_reply_to_status_id_str"]})
                if parentTweet is None:
                    print("parentNone")
                    tweets_to_collect.replace_one({"_id": tweetCheck["in_reply_to_status_id_str"]}, {
                                                "build_tree": True}, True)
                    tweet_tree.update_one(
                        {"_id": tweetCheck["_id"]},
                        {
                            "$set": {
                                "scrape_status": str(statusCheckNum)
                            }
                        }
                    )
                else:
                    if "scrape_status" in parentTweet and parentTweet["scrape_status"] == "Root":
                        tweet_tree.update_one(
                            {"_id": parentTweet["_id"]},
                            {
                                "$addToSet": {
                                    "hashtagsInTree": { "$each" : list(hashtags_in_tree)} ,
                                    "usersInTree": { "$each" : list(users_in_tree) }
                                }
                            }
                        )
                        tweet_tree.update_one(
                            {"_id": tweetCheck["_id"]},
                            {
                                "$set": {
                                    "scrape_status": "Linked"
                                },
                                "$addToSet": {
                                    "ancestors": parentTweet["_id"]
                                }
                            }
                        )
                    else:
                        alphaTweet = tweet_tree.find_one(
                            {
                                "scrape_status": "Root",
                                "_id" : { "$in": parentTweet["ancestors"] }
                            }
                        )
                        if alphaTweet is not None:
                            tweet_tree.update_one(
                                {"_id": alphaTweet["_id"]},
                                {
                                    "$addToSet": {
                                        "hashtagsInTree": { "$each" : list(hashtags_in_tree) },
                                        "usersInTree": { "$each" : list(users_in_tree) }
                                    }
                                }
                            )
                            ancest = parentTweet["ancestors"]
                            ancest.append(parentTweet["_id"])
                            tweet_tree.update_one(
                                {"_id": tweetCheck["_id"]},
                                {
                                    "$set": {
                                        "scrape_status": "Linked"
                                    },
                                    "$addToSet": {
                                        "ancestors": { "$each" : ancest}
                                    }
                                }
                            )
                        else:
                            print(parentTweet["ancestors"])
                            tweet_tree.update_one(
                                {"_id": tweetCheck["_id"]},
                                {
                                    "$set": {
                                        "scrape_status": str(statusCheckNum)
                                    }
                                }
                            )