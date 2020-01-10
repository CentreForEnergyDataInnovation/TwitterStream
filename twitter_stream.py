from datetime import datetime
import pymongo
import pytz

from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterResponse

from TwitterAPIAlt.StreamingIterable import get_iterator_override

from twitter_api_constants import *

def prepare_trackers(api, hashtags_db, users_db, settings_db):
    hashtags = []
    for x in hashtags_db.find():
        hashtags.append(x["term"])
    
    new_users = []
    for x in users_db.find({ "checked" : False }):
        new_users.append(x["screen_name"])

    if new_users:
        r = api.request("users/lookup", {"screen_name": (",").join(new_users)})

        for item in r:
            users_db.replace_one({"screen_name" : item["screen_name"]}, item)

    users = []
    for x in users_db.find({ "id_str" : { "$exists" : True } }):
        users.append(x["id_str"])
    
    settings_db.replace_one({ "updated" : True }, { "updated" : False })

    return [(",").join(hashtags), (",").join(users)]

def process_tweet(tweet):
    retweet = False
    quote = False

    if "retweeted_status" in tweet:
        process_tweet(tweet["retweeted_status"])
        retweet = True
    
    if "quoted_status" in tweet:
        process_tweet(tweet["quoted_status"])
        del tweet["quoted_status"]
        quote = True
    
    tweet["_id"] = tweet["id_str"]
    tweet["user"]["_id"] = tweet["user"]["id_str"]
    
    if retweet == False:
        users.replace_one({ "_id" : tweet["user"]["_id"] }, tweet["user"], True)

        source_tweet = {
            "_id" : tweet["_id"],
            "user_id_str" : tweet["user"]["_id"],
            "screen_name" : tweet['user']['screen_name'],
            "quoter" : quote,
            "scrape_time" : datetime.utcnow()
        }
        users_to_search.replace_one({ "_id" : tweet["_id"] }, source_tweet, True)
        
        tweets.replace_one({ "_id" : tweet["_id"] }, tweet, True)

        tweet_text = ""
        if "extended_tweet" in tweet:
            tweet_text = tweet["extended_tweet"]["full_text"]
        else:
            tweet_text = tweet["text"]

        tweet_node = {
            "_id" : tweet["_id"],
             "tweet_text" : tweet_text,
             "user_id_str" : tweet["user"]["_id"],
             "in_reply_to_status_id" : tweet["in_reply_to_status_id"],
             "quoted_status_id_str" : tweet["quoted_status_id_str"] if "quoted_status_id_str" in tweet else None,
             "created_at" : tweet["created_at"],
             "created_at_dt" : datetime.strptime(tweet["created_at"],'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC),
             "entities" : tweet["entities"],
             "quote_count" : tweet["quote_count"],
             "reply_count" : tweet["reply_count"],
             "retweet_count" : tweet["retweet_count"],
             "favorite_count" : tweet["favorite_count"],
             "ancestors" : []
        }

        if tweet_tree.count_documents({ "_id" : tweet["_id"] }) == 0:
            tweet_tree.insert_one(tweet_node)
            print("Insert Tree Node")
        else:
            print("Update Tree Node")
            tweet_tree.update_one(
                { "_id" : tweet["_id"] },
                {
                    "$set" : {
                        "quote_count" : tweet["quote_count"],
                        "reply_count" : tweet["reply_count"],
                        "retweet_count" : tweet["retweet_count"],
                        "favorite_count" : tweet["favorite_count"]
                    }
                }
            )

# Set up Twitter API

api = TwitterAPI(
    CONSUMER_KEY,
    CONSUMER_SECRET,
    ACCESS_TOKEN_KEY,
    ACCESS_TOKEN_SECRET
)

# Set up global search terms

TRACK_TERM = ""
FOLLOW_USER = ""

# Set up Mongo

MONGO = pymongo.MongoClient("mongodb://localhost:27017")
twitter_db = MONGO["twitters"]
trackers_hashtags = twitter_db["trackersHashtags"]
trackers_users = twitter_db["trackersUsers"]
update_settings = twitter_db["updated"]

tweets = twitter_db["tweets"]
users = twitter_db["users"]
users_to_search = twitter_db["usersToSearch"]
tweets_to_collect = twitter_db["tweetsToCollect"]
tweet_tree = twitter_db["tweetTree"]

while True:

    # Set up tracking terms

    tracking = prepare_trackers(api, trackers_hashtags, trackers_users, update_settings)
    TRACK_TERM = tracking[0]
    FOLLOW_USER = tracking[1]

    print(TRACK_TERM)
    print(FOLLOW_USER)

    # Override

    TwitterResponse.get_iterator = get_iterator_override

    r = api.request("statuses/filter", {"track" : TRACK_TERM, "follow" : FOLLOW_USER})

    for item in r:
        if "stop" not in item:
            process_tweet(item)

        checking = update_settings.find_one()

        if checking["updated"] == True:
            break