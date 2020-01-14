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
        tweetCheck = tweet_tree.find(
            {
                "scrape_status": {"$nin": ["Root", "Linked", str(statusCheckNum)]},
            }
        ).sort("_id", 1).collation(Collation(locale="en_US", numericOrdering=True)).limit(1)

        if tweetCheck.count() == 0:
            print("cycle")
            break

        print("tree building: " + tweetCheck[0]["_id"])

        hashtags_in_tree = set()
        users_in_tree = set()

        users_in_tree.add(tweetCheck[0]["user_id_str"])

        for x in tweetCheck[0]["entities"]["hashtags"]:
            hashtags_in_tree.add(x["text"].lower())
        for x in tweetCheck[0]["entities"]["user_mentions"]:
            users_in_tree.add(x["id_str"])

        if tweetCheck[0]["quoted_status_id_str"] is not None:
            quoteTweet = tweet_tree.find_one(
                {"_id": tweetCheck[0]["quoted_status_id_str"]})
            if quoteTweet is None:
                tweets_to_collect.replace_one({"_id": tweetCheck[0]["quoted_status_id_str"]}, {
                                            "_id": tweetCheck[0]["quoted_status_id_str"]}, True)
                tweet_tree.update_one(
                    {"_id": tweetCheck[0]["_id"]},
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

        if tweetCheck[0]["in_reply_to_status_id_str"] is None:
            tweet_tree.update_one(
                {"_id": tweetCheck[0]["_id"]},
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
                {"_id": tweetCheck[0]["in_reply_to_status_id_str"]})
            if parentTweet is None:
                tweets_to_collect.replace_one({"_id": tweetCheck[0]["in_reply_to_status_id_str"]}, {
                                            "build_tree": True}, True)
                tweet_tree.update_one(
                    {"_id": tweetCheck[0]["_id"]},
                    {
                        "$set": {
                            "scrape_status": str(statusCheckNum)
                        }
                    }
                )
            else:
                if parentTweet["scrape_status"] == "Root":
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
                        {"_id": tweetCheck[0]["_id"]},
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
                            {"_id": tweetCheck[0]["_id"]},
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
                        tweet_tree.update_one(
                            {"_id": tweetCheck[0]["_id"]},
                            {
                                "$set": {
                                    "scrape_status": str(statusCheckNum)
                                }
                            }
                        )

    while tweets_to_collect.count_documents({}) > 0:

        tweetsToCollect = []
        for item in tweets_to_collect.find().sort("_id", 1).collation(Collation(locale="en_US", numericOrdering=True)).limit(100):
            tweetsToCollect.append(item["_id"])
            tweets_to_collect.delete_one({"_id" : item["_id"]})
        
        tweetsToSearch = (",").join(tweetsToCollect)
        print("reply lookup: " +tweetsToSearch)
        r = api.request("statuses/lookup", { "id" : tweetsToSearch, "tweet_mode" : "extended", "map" : True})

        for item in r:
            for subitem in item["id"]:
                if item["id"][subitem] is not None:
                    process_tweet(item["id"][subitem], users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                else:
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

