import requests
import os
import json
from config import *
from class_TweetStreamer import *


def main():

    # tweetStreamer object
    tweetStreamer = TweetStreamer(BEARER_TOKEN)

    # headers is the authentication dict
    #headers = create_headers(BEARER_TOKEN)

    # GET default rules
    # old_rules = get_rules(headers, BEARER_TOKEN)
    # for key in old_rules:
    #    print(key)
    rules = tweetStreamer.get_rules()

    # POST delete request
    # delete = delete_all_rules(headers, BEARER_TOKEN, old_rules)

    # POST new rules
    # set_new_rules = set_rules(headers, delete, BEARER_TOKEN)

    # GET new rules
    #new_rules = get_rules(headers, BEARER_TOKEN)
    # for key in new_rules:
    #    print(key)

    # get_stream(headers, set_new_rules, BEARER_TOKEN)
    tweetStreamer.get_tweet_stream(rules)


if __name__ == "__main__":
    main()
