import sys
import requests
import os
import json
from TweetLoader import *


class TweetStreamer:

    # ======== CONSTRUCTOR
    def __init__(self, bearer_token, database_url):

        # ======== INSTANCE VARIABLES
        self.authentication_header = self.create_headers(bearer_token)

        self.tweet_loader = TweetLoader(database_url)

    # ======== METHODS

    """
    Creates a key-value pair for authorization
    """

    def create_headers(self, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    """
    Makes a GET request and returns the streaming rules
    """

    def get_rules(self):

        # GET request to retrieve all the rules (default is no rules)
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", headers=self.authentication_header
        )

        # raise HTTP status exception
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(
                    response.status_code, response.text)
            )

        # inspect the json response object
        print(json.dumps(response.json()))

        # return the json response object
        return response.json()

    """
    Makes a POST request and deletes all the rules currently set.
    """

    def delete_all_rules(self, rules):
        if rules is None or "data" not in rules:
            return None

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            headers=self.authentication_header,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print(json.dumps(response.json()))

    """
    Makes a POST request and adds rules to stream listener
    """

    def set_rules(self, custom_rules, delete):

        # sample_rules = [
        #    {"value": "dog has:images", "tag": "dog pictures"},
        #    {"value": "cat has:images -grumpy", "tag": "cat pictures"},
        # ]

        # payload manages the addition
        payload = {"add": custom_rules}

        # POST request
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            headers=self.authentication_header,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(
                    response.status_code, response.text)
            )
        # print(json.dumps(response.json()))

    def get_tweet_stream(self, recreate_db=False):

        # build url to make a GET request for
        filtered_stream_endpoint = "https://api.twitter.com/2/tweets/search/stream"
        tweet_fields = "?tweet.fields=created_at,lang"
        tweet_expansions = "&expansions=geo.place_id&"
        places_fields = "&place.fields=country,country_code,full_name,geo,id,name,place_type"
        url = filtered_stream_endpoint + tweet_fields + tweet_expansions + places_fields

        print("url: ", url)

        # GET request
        response = requests.get(
            url, headers=self.authentication_header, stream=True)

        # confirm if request has gone through
        print(response.status_code)

        # raise exception if not
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        # use tweetloader object to transform and load the data to db
        for response_line in response.iter_lines():
            if response_line:
                # convert json to python dict
                json_response = json.loads(response_line)

                # load json response to db
                self.tweet_loader.transform_and_load(
                    json_response, recreate_db)
