from datetime import datetime
import pymongo
import pytz

def process_tweet(tweet, users, users_to_search, tweets, tweet_tree, tweets_to_collect):

    retweet = False
    quote = False

    quoteTweet = []

    if "retweeted_status" in tweet:
        process_tweet(tweet["retweeted_status"], users, users_to_search, tweets, tweet_tree, tweets_to_collect)
        retweet = True
    
    if "quoted_status" in tweet:
        process_tweet(tweet["quoted_status"], users, users_to_search, tweets, tweet_tree, tweets_to_collect)
        quoteTweet = tweet["quoted_status"]
        del tweet["quoted_status"]
        quote = True
    
    try:
        tweet["_id"] = tweet["id_str"]
    except:
        print(tweet)
        raise

    if tweet["truncated"] == True:
        tweets_to_collect.replace_one({"_id": tweet["id_str"]}, {"_id": tweet["id_str"]}, True)
    
    tweet["user"]["_id"] = tweet["user"]["id_str"]
    
    if retweet == False:
        users.replace_one({ "_id" : tweet["user"]["_id"] }, tweet["user"], True)

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

        alreadyExist = tweets.find_one({ "_id" : tweet["_id"] })

        if alreadyExist is None:
            tweets.insert_one(tweet)
        else:
            if "collect" in alreadyExist:
                tweets.replace_one({"_id" : alreadyExist["_id"]}, tweet)
            else:
                tweets.update_one(
                    { "_id" : tweet["_id"] },
                    {
                        "$set" : {
                            "retweet_count" : tweet["retweet_count"] if "retweet_count" in tweet else 0,
                            "favorite_count" : tweet["favorite_count"] if "favorite_count" in tweet else 0
                        }
                    }
                )

        tweet_text = ""
        if "extended_tweet" in tweet:
            tweet_text = tweet["extended_tweet"]["full_text"]
        elif "full_text" in tweet:
            tweet_text = tweet["full_text"]
        else:
            tweet_text = tweet["text"]

        tweet_node = {
            "_id" : tweet["_id"],
             "tweet_text" : tweet_text,
             "user_id_str" : tweet["user"]["_id"],
             "in_reply_to_status_id_str" : tweet["in_reply_to_status_id_str"],
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

            if alreadyExist is not None and alreadyExist["truncated"] == True:
                tweet_tree.update_one(
                    { "_id" : tweet["_id"] },
                    {
                        "$set" : {
                            "tweet_text" : tweet_text,
                            "quote_count" : tweet["quote_count"] if "quote_count" in tweet else 0,
                            "reply_count" : tweet["reply_count"] if "reply_count" in tweet else 0,
                            "retweet_count" : tweet["retweet_count"] if "retweet_count" in tweet else 0,
                            "favorite_count" : tweet["favorite_count"] if "favorite_count" in tweet else 0,
                        }, 
                        "$unset" : {
                            "cleanCheck" : ""
                        }
                    }
                )
            else:
                tweet_tree.update_one(
                    { "_id" : tweet["_id"] },
                    {
                        "$set" : {
                            "tweet_text" : tweet_text,
                            "quote_count" : tweet["quote_count"] if "quote_count" in tweet else 0,
                            "reply_count" : tweet["reply_count"] if "reply_count" in tweet else 0,
                            "retweet_count" : tweet["retweet_count"] if "retweet_count" in tweet else 0,
                            "favorite_count" : tweet["favorite_count"] if "favorite_count" in tweet else 0,
                        }
                    }
                )


            

        if tweet["in_reply_to_status_id_str"] is not None:
            parent = tweet_tree.find_one({ "_id" : tweet["in_reply_to_status_id_str"] })
            if parent is None:
                tweets_to_collect.replace_one({"_id": tweet["in_reply_to_status_id_str"]}, {
                                            "_id": tweet["in_reply_to_status_id_str"]}, True)
            else:
                hashtags_in_tree = set()
                users_in_tree = set()

                users_in_tree.add(tweet["user"]["id_str"])

                for x in tweet["entities"]["hashtags"]:
                    hashtags_in_tree.add(x["text"].lower())
                for x in tweet["entities"]["user_mentions"]:
                    users_in_tree.add(x["id_str"])

                if quote == True:
                    for x in quoteTweet["entities"]["hashtags"]:
                        hashtags_in_tree.add(x["text"].lower())
                    for x in quoteTweet["entities"]["user_mentions"]:
                        users_in_tree.add(x["id_str"])
                    users_in_tree.add(quoteTweet["user"]["id_str"])

                if "scrape_status" in parent and parent["scrape_status"] == "Root":
                    tweet_tree.update_one(
                        {"_id": parent["_id"]},
                        {
                            "$addToSet": {
                                "hashtagsInTree": { "$each" : list(hashtags_in_tree)} ,
                                "usersInTree": { "$each" : list(users_in_tree) }
                            }
                        }
                    )
                    tweet_tree.update_one(
                        {"_id": tweet["_id"]},
                        {
                            "$set": {
                                "scrape_status": "Linked"
                            },
                            "$addToSet": {
                                "ancestors": parent["_id"]
                            }
                        }
                    )
                else:
                    alphaTweet = tweet_tree.find_one(
                        {
                            "scrape_status": "Root",
                            "_id" : { "$in": parent["ancestors"] }
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
                        ancest = parent["ancestors"]
                        ancest.append(parent["_id"])
                        tweet_tree.update_one(
                            {"_id": tweet["_id"]},
                            {
                                "$set": {
                                    "scrape_status": "Linked"
                                },
                                "$addToSet": {
                                    "ancestors": { "$each" : ancest}
                                }
                            }
                        )


        return tweet["_id"]