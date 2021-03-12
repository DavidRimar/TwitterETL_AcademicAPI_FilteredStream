import requests
import os
import json
from config import *
from TweetStreamer import *


def start_filter_stream():

    # tweetStreamer object
    tweetStreamer = TweetStreamer(BEARER_TOKEN, DATABASE_URI_TRIAL)

    # GET currently set rules
    current_rules = tweetStreamer.get_rules()

    # POST delete request (uncomment to delete all existing rules)
    deleted_rules = tweetStreamer.delete_all_rules(current_rules)

    # define new rules
    custom_rules = [{"value": "(meghan OR harry) lang:en place_country:GB -is:retweet",
                     "tag": "Meghan or Harry in GB"}]

    # POST new rules (uncomment if new rules are created)
    new_rules = tweetStreamer.set_rules(custom_rules, deleted_rules)

    # GET tweets with tweetStreamer
    # It also handles data loading to DB via its tweetLoader instance
    # set recreate_db = True if ran for the first time
    tweetStreamer.get_tweet_stream(recreate_db=True)


def main():

    # start the filter stream
    start_filter_stream()


if __name__ == "__main__":
    main()
