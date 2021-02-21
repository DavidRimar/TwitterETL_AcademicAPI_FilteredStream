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

    # POST delete request
    # deleted_rules = tweetStreamer.delete_all_rules(current_rules)

    # define rules here
    custom_rules = [{"value": "(flood OR flooding OR flooded) lang:en place_country:GB -is:retweet",
                     "tag": "flood-related keywords in GB"}]

    # POST new rules
    # new_rules = tweetStreamer.set_rules(custom_rules, deleted_rules)

    # GET tweets with the rules passed in
    tweetStreamer.get_tweet_stream(current_rules)


def main():

    # start the filter stream
    start_filter_stream()


if __name__ == "__main__":
    main()
