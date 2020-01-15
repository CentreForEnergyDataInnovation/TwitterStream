from datetime import datetime
import pymongo
import pytz

from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterResponse

from TwitterAPIAlt.StreamingIterable import get_iterator_override

from twitter_api_constants import *

from process_tweet import process_tweet

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
exceptions = twitter_db["exceptions"]

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
            if "id_str" in item:
                process_tweet(item, users, users_to_search, tweets, tweet_tree, tweets_to_collect)
            else:
                exceptions.insert_one(item)

        checking = update_settings.find_one()

        if checking["updated"] == True:
            break