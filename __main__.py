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
    custom_rules = [{"value": "(flood OR flooding OR flooded) lang:en place_country:GB -is:retweet",
                     "tag": "flood-related keywords in GB"}]

    # POST new rules (uncomment if new rules are created)
    new_rules = tweetStreamer.set_rules(custom_rules, deleted_rules)

    # GET tweets with tweetStreamer
    # It also handles data loading to DB via its tweetLoader
    # set create_db = True for the first time the script is ran
    # set recreate_db = True if we need to recreate schema
    tweetStreamer.get_tweet_stream()


def main():

    # start the filter stream
    start_filter_stream()


if __name__ == "__main__":
    main()
