from Tweet import *
from config import *
from sqlalchemy import create_engine
from datetime import datetime
from Tweet import Base, Tweet
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import json

"""
The TweetLoader class takes care of transforming the fields from a response
to adhere to the data schema represented by the Tweet class.
It uses sqlAlchemy to load the tweets to a DB.
"""


class TweetLoader():

    # CONSTRUCTOR
    def __init__(self, database_url):

        # INSTANCE VARIABLES
        self.recreatedDB = False

        # an engine to communicate with PostgreSQL
        self.engine = create_engine(database_url)

        # a Session object to manage connections (session starts)
        self.Session = sessionmaker(bind=self.engine)

    ### METHODS ###

    # 1. start_load()
    # 2. transform_and_load()
    # 3. create_database()
    # 4. recreate_database()
    # 5. session_scope()

    # START LOAD
    # 1.
    def start_load(self, tweet_to_add, recreate_db):

        print("loading started!")

        # if only interested in the new data, recreate_db deletes data streamed before
        if recreate_db == True and self.recreatedDB == False:
            self.recreate_database()
            print("recreate db ran")
            self.recreatedDB = True

        # connect to DB with session
        with self.session_scope() as s:

            # add tweet to DB
            s.add(tweet_to_add)

            print("loading successful!")

    # TRANSFORM AND LOAD
    # 2.
    def transform_and_load(self, json_response, recreate_db):

        # inspect response line (optional)
        print("json printed: ", json.dumps(
            json_response, indent=4, sort_keys=True))

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

        # inspect transformed Tweet() object
        print("single_tweet: ", single_tweet)

        # load data
        self.start_load(single_tweet, recreate_db)

    # CREATE DATABASE
    # 4.
    def create_database(self):

        # creates all tables
        Base.metadata.create_all(self.engine)

    # RECREATE DATABASE
    # 4.
    def recreate_database(self):

        # drops all tables
        Base.metadata.drop_all(self.engine)

        # creates all tables
        Base.metadata.create_all(self.engine)

    # A CONTEXT MANAGER
    # 5.
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
