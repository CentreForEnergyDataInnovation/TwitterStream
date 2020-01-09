import datetime
import pymongo

from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterPager

from twitter_api_constants import *

api = TwitterAPI(
    CONSUMER_KEY,
    CONSUMER_SECRET,
    auth_type="oAuth2"
)

tweet_count = 0
retweet_count = 0
total_count = 0

r = TwitterPager(api, "search/tweets", { "q" : SEARCH_TERM, "count" : SEARCH_COUNT, "result_type" : "recent" })

for item in r.get_iterator():
    total_count += 1
    if "retweeted_status" in item:
        retweet_count += 1
    else:
        tweet_count += 1
    
    if "message" in item and item["code"] == 88:
        print("RATELIMIT")
        print(item["message"])
        break

    print(str(total_count) + " : " + str(tweet_count) + " : " + str(retweet_count))

print("Done")