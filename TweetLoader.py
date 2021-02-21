# ONCE THE TWEETS ARE EXTRACTED, HERE WE TRANSFORM AND LOAD THE DATA TO THE DB
from Tweet import *
from config import *
from sqlalchemy import create_engine
from datetime import datetime
from Tweet import Base, Tweet
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import json


class TweetLoader():

    # CLASS VARIABLES

    # CONSTRUCTOR
    def __init__(self, database_url):

        # INSTANCE VARIABLES

        # raw_tweet_list: a list containing the raw Tweet objects
        self.raw_tweet_list = list()

        # transformed_tweet_list: a list containing the transformed Tweet objects
        self.transformed_tweet_list = list()

        # an engine to communicate with PostgreSQL
        self.engine = create_engine(database_url)

        # a Session object to manage connections (session starts)
        self.Session = sessionmaker(bind=self.engine)

    ### METHODS ###

    # 1. start_load()
    # 2. transform_and_load()
    # 3. recreate_database()
    # 4. session_scope()

    def start_load(self, tweet_to_add):

        #
        print("loading started!")

        # connect to DB

        # session
        with self.session_scope() as s:
            # add the first element of the list
            s.add(tweet_to_add)

        # add transformedTweetList to DB
        # s.add(tweet) # iteration required over the Tweets list

    # TRANSFORM AND LOAD

    def transform_and_load(self, json_response):

        # iterate over the response
        # for response_line in jsonResponse.iter_lines():
        #  if response_line:
        # save response in JSON
        # json_response = json.loads(response_line)

        # inspect response line
        print("json printed: ", json.dumps(
            json_response, indent=4, sort_keys=True))

        print("json.dumps type: ", type(json.dumps(json_response)))
        print("json_response Type: ", type(json_response))

        # dismantle the fields
        tweet_id = json_response["data"]["id"]
        tweet_text = json_response["data"]["text"]
        tweet_lang = json_response["data"]["lang"]
        tweet_created_at = json_response["data"]["created_at"]
        tweet_place_id = json_response["includes"]["places"][0]["id"]
        tweet_place_geo_bbox = json_response["includes"]["places"][0]["geo"]["bbox"]
        tweet_place_full_name = json_response["includes"]["places"][0]["full_name"]
        tweet_place_type = json_response["includes"]["places"][0]["place_type"]
        tweet_country_code = json_response["includes"]["places"][0]["country_code"]
        stream_rule_tag = json_response["matching_rules"][0]["tag"]

        # construct tweet_data_dict
        tweet_data_dict = {'twitter_id': tweet_id,
                           'text': tweet_text,
                           'lang': tweet_lang,
                           'created_at': tweet_created_at,
                           'places_geo_place_id': tweet_place_id,
                           'places_geo_bbox': tweet_place_geo_bbox,
                           'places_full_name': tweet_place_full_name,
                           'places_place_type': tweet_place_type,
                           'places_country_code': tweet_country_code,
                           'stream_rule_tag': stream_rule_tag}

        # construct a Tweet() object
        # data passed in to Tweet() has to be in a dictionary format
        single_tweet = Tweet(**tweet_data_dict)

        # add Tweet to tweetList
        # self.transformed_tweet_list.append(single_tweet)

        print("Transformation complete!")
        print("single_tweet", single_tweet)

        # load data
        self.start_load(single_tweet)

    # RECREATE DATABASE

    def recreate_database(self):

        # drops all tables
        Base.metadata.drop_all(self.engine)

        # creates all tables
        Base.metadata.create_all(self.engine)

    # A CONTEXT MANAGER

    @ contextmanager
    def session_scope(self):

        # local scope creates and uses a session
        session = self.Session()  # invokes sessionmaker.__call__()

        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


"""
for item_dict in myStreamListener.streamed_data_array:
    # print("Tweet text starts: \n", item_dict['text'])

    # dismantle the JSON dictionary and format if needed
    twitter_id = item_dict['id']  # Twitter ID field
    text = item_dict['text']  # Twitter text field
    lang = item_dict['lang']  # Twitter lang field
    created_at = item_dict['created_at']  # DateTime

    # Column(String)  # Twitter geo.place_id field
    geo_place_id = item_dict['geo']  # most of the time string: "None"

    geo_coord_type = "None"
    geo_coord_coord_1 = 0
    geo_coord_coord_2 = 0

    if geo_place_id != None:

        geo_coord_type = item_dict['geo.coordinates.type']

        # Column(Integer)
        geo_coord_coord_1 = item_dict['geo.coordinates.coordinates'][0]

        # Column(Integer)
        geo_coord_coord_2 = item_dict['geo.coordinates.coordinates'][1]
    else:
        print("It is none!")
    """
