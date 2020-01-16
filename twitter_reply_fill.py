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
poll_seed = twitter_db["pollSeed"]
pollreply_seed = twitter_db["pollReplySeed"]

tweets_staging = twitter_db["tweets_staging"]

statusCheckNum = 0
cyclecount = 0

while True:

    pollSeedCheck = pollreply_seed.find_one()
    if pollSeedCheck == None:
        pollreply_seed.insert_one({ "statusCheck" : str(statusCheckNum) })
    else:
        oldStatusCheckNum = int(pollSeedCheck["statusCheck"])
        statusCheckNum = (oldStatusCheckNum + 1) % 5
        pollreply_seed.replace_one({ "statusCheck" : str(oldStatusCheckNum)}, { "statusCheck" : str(statusCheckNum)})

    while True:
        sCount = users_to_search.count_documents({"valid" : True, "reply_search_status": {"$nin": ["Expired", "Tracked", "Prune", str(statusCheckNum)]}})
        toSearch = users_to_search.find_one({"valid" : True, "reply_search_status": {"$nin": ["Expired", "Tracked", "Prune", str(statusCheckNum)]}}, sort=[("_id", 1)], collation = Collation(locale="en_US", numericOrdering=True))
        cyclecount += 1

        if toSearch is None:
            cyclecount = 0
            print("cycle")
            break

        user_id_str = toSearch["user_id_str"]
        screen_name = toSearch["screen_name"]
        created_at = toSearch["created_at"]

        source_tweet_id = toSearch["_id"]



        if trackers_users.find_one({"id_str" : user_id_str}) is not None or screen_name == "realDonaldTrump":
            print("Tracked: "+screen_name)
            users_to_search.update_many(
                { "user_id_str" : user_id_str },
                {
                    "$set" : {
                        "reply_search_status" : "Tracked"
                    }
                }
            )
            continue

        tweetIDs = []

        for u in users_to_search.find({ "user_id_str" : toSearch["user_id_str"], "reply_search_status" : { "$ne" : "Expired" } }):
            if "newCheckpoint" in u and u["newCheckpoint"] is not None:
                tweetIDs.append(u["newCheckpoint"])
            else:
                tweetIDs.append(u["_id"])
            
            a = pytz.utc.localize(u["created_at_dt"])
            b = datetime.now(timezone.utc)
            timediff = b - a

            if timediff.days > 6:
                users_to_search.update_one(
                    { "_id" : u["_id"] },
                    {
                        "$set" : {
                            "reply_search_status" : "Expired"
                        }
                    }
                )
        
        users_to_search.update_many(
            {
                "user_id_str" : toSearch["user_id_str"],
                "reply_search_status" : { "$ne" :"Expired" }
            },
            {
                "$set" : {
                    "reply_search_status" : str(statusCheckNum)
                }
            }
        )

        tCount = len(tweetIDs)

        minSnowflake = min(tweetIDs, key=bson.int64.Int64)

        searching_term = "((from:"+screen_name+") OR (to:"+screen_name+") OR (@"+screen_name+")) -filter:retweets"

        r = TwitterPager(api, "search/tweets", {
            "q" : searching_term,
            "count" : SEARCH_COUNT,
            "since_id" : minSnowflake,
            "result_type" : "recent",
            "tweet_mode" : "extended"
        })

        tweet_count = 0
        staging_count = 0

        oldest = None

        for item in r.get_iterator(wait = 2.1):

            if oldest is None:
                oldest = item["id_str"]

            tweet_count += 1

            tweet_id = item["id_str"]
            reply_to = item["in_reply_to_status_id_str"]
            quote_of = item["quoted_status_id_str"] if "quoted_status_id_str" in item else None

            item["_id"] = tweet_id

            newcreated_at = item["created_at"]

            if reply_to is None and quote_of is None:
                print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - void " + tweet_id + " - " + newcreated_at)
                continue

            if tweet_tree.find_one({ "_id" : tweet_id }) is not None:
                print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - already captured " + tweet_id + " - " + newcreated_at)
                process_tweet(item, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                continue

            if reply_to is not None and quote_of is not None:
                if tweet_tree.find_one({ "_id" : { "$in" : [reply_to, quote_of] } }) is None:
                    print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - staging " + tweet_id + " - " + newcreated_at)
                    tweets_staging.insert_one(item)
                    staging_count += 1
                    continue
                else:
                    print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - capture reply/quote " + tweet_id + " - " + newcreated_at)
                    process_tweet(item, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                    continue

            if reply_to is not None:
                if tweet_tree.find_one({ "_id" : reply_to }) is None:
                    print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - staging " + tweet_id + " - " + newcreated_at)
                    tweets_staging.insert_one(item)
                    staging_count += 1
                    continue
                else:
                    print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - capture reply " + tweet_id + " - " + newcreated_at)
                    process_tweet(item, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                    continue

            if quote_of is not None:
                if tweet_tree.find_one({ "_id" : quote_of }) is None:
                    print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - staging " + tweet_id + " - " + newcreated_at)
                    tweets_staging.insert_one(item)
                    staging_count += 1
                    continue
                else:
                    print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - " + str(tweet_count) + " - capture quote " + tweet_id + " - " + newcreated_at)
                    process_tweet(item, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                    continue

        print(screen_name + " - finished consumption of api - starting staging parse")

        while True:

            
            for staging_tweet in tweets_staging.find({}, sort=[("_id", 1)], collation = Collation(locale="en_US", numericOrdering=True)).limit(1000):
                staging_count -= 1

                #tweet_id = staging_tweet["_id"]
                reply_to = staging_tweet["in_reply_to_status_id_str"]
                quote_of = staging_tweet["quoted_status_id_str"] if "quoted_status_id_str" in staging_tweet else None

                tweets_staging.delete_one({ "_id" : staging_tweet["_id"] })

                newcreated_at = staging_tweet["created_at"]

                if reply_to is not None and quote_of is not None:
                    if tweet_tree.find_one({ "_id" : { "$in" : [reply_to, quote_of] } }) is None:
                        print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - staging - " + str(staging_count) + " - void " + staging_tweet["_id"] + " - " + newcreated_at)
                        continue
                    else:
                        print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - staging - " + str(staging_count) + " - capture reply/quote " + staging_tweet["_id"] + " - " + newcreated_at)
                        process_tweet(staging_tweet, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                        continue

                if reply_to is not None:
                    if tweet_tree.find_one({ "_id" : reply_to }) is None:
                        print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - staging - " + str(staging_count) + " - void " + staging_tweet["_id"] + " - " + newcreated_at)
                        continue
                    else:
                        print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - staging - " + str(staging_count) + " - capture reply " + staging_tweet["_id"] + " - " + newcreated_at)
                        process_tweet(staging_tweet, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                        continue

                if quote_of is not None:
                    if tweet_tree.find_one({ "_id" : quote_of }) is None:
                        print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - staging - " + str(staging_count) + " - void " + staging_tweet["_id"] + " - " + newcreated_at)
                        continue
                    else:
                        print(str(sCount)+"("+str(tCount)+")" + " " + str(cyclecount) + " - " + screen_name + " | " + source_tweet_id + " - " + created_at + " - staging - " + str(staging_count) + " - capture quote " + staging_tweet["_id"] + " - " + newcreated_at)
                        process_tweet(staging_tweet, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
                        continue

            if tweets_staging.count_documents({}) > 0:
                continue

            print(screen_name + " - finished staging parsing")
            break

        if oldest is None:
            users_to_search.update_many(
                {
                    "user_id_str" : toSearch["user_id_str"],
                    "reply_search_status" : { "$ne" :"Expired" }
                },
                {
                    "$set" : {
                        "reply_search_status" : str(statusCheckNum)
                    }
                }
            )
        else:
            users_to_search.update_many(
                {
                    "user_id_str" : toSearch["user_id_str"],
                    "reply_search_status" : { "$ne" :"Expired" }
                },
                {
                    "$set" : {
                        "reply_search_status" : str(statusCheckNum),
                        "newCheckpoint" : oldest
                    }
                }
            )
        time.sleep(2.5)