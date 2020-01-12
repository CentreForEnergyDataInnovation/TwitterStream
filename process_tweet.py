from datetime import datetime
import pymongo
import pytz

def process_tweet(tweet, users, users_to_search, tweets, tweet_tree):

    retweet = False
    quote = False

    if "retweeted_status" in tweet:
        process_tweet(tweet["retweeted_status"], users, users_to_search, tweets, tweet_tree)
        retweet = True
    
    if "quoted_status" in tweet:
        process_tweet(tweet["quoted_status"], users, users_to_search, tweets, tweet_tree)
        del tweet["quoted_status"]
        quote = True
    
    tweet["_id"] = tweet["id_str"]
    tweet["user"]["_id"] = tweet["user"]["id_str"]
    
    if retweet == False:
        users.replace_one({ "_id" : tweet["user"]["_id"] }, tweet["user"], True)

        """
        source_tweet = {
            "_id" : tweet["_id"],
            "user_id_str" : tweet["user"]["_id"],
            "screen_name" : tweet['user']['screen_name'],
            "quoter" : quote,
            "scrape_time" : datetime.utcnow()
        }
        users_to_search.replace_one({ "_id" : tweet["_id"] }, source_tweet, True)
        """

        users_to_search.update_one(
            { "_id" : tweet["_id"] },
            {
                "$set" : {
                    "user_id_str" : tweet["user"]["_id"],
                    "screen_name" : tweet['user']['screen_name'],
                    "created_at" : tweet["created_at"],
                    "created_at_dt" : datetime.strptime(tweet["created_at"],'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC),
                    "quoter" : quote,
                    "scrape_time" : datetime.utcnow()
                }
            },
            True
        )
        
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
             "quote_count" : tweet["quote_count"] if "quote_count" in tweet else 0,
             "reply_count" : tweet["reply_count"] if "reply_count" in tweet else 0,
             "retweet_count" : tweet["retweet_count"] if "retweet_count" in tweet else 0,
             "favorite_count" : tweet["favorite_count"] if "favorite_count" in tweet else 0,
             "ancestors" : []
        }

        if tweet_tree.count_documents({ "_id" : tweet["_id"] }) == 0:
            tweet_tree.insert_one(tweet_node)
            #print("Insert Tree Node")
        else:
            #print("Update Tree Node")
            tweet_tree.update_one(
                { "_id" : tweet["_id"] },
                {
                    "$set" : {
                        "quote_count" : tweet["quote_count"] if "quote_count" in tweet else 0,
                        "reply_count" : tweet["reply_count"] if "reply_count" in tweet else 0,
                        "retweet_count" : tweet["retweet_count"] if "retweet_count" in tweet else 0,
                        "favorite_count" : tweet["favorite_count"] if "favorite_count" in tweet else 0,
                    }
                }
            )

        return tweet["_id"]