from class_DataLoader import *
import sys
import tweepy
from config import *
from class_StreamListener import MyStreamListener
from class_ConnectTwitter import *


def main():

    # 1. EXTRACT DATA
    # 2. TRANSFORM DATA
    # 3. LOAD DATA

    ################## 1. EXTRACT DATA #########################

    # myStreamListener instance will store the streamed tweets
    stream_listener = MyStreamListener()

    #  as the stream sends data to it
    data_extractor = ConnectTwitter(
        stream_listener, API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # stream filters (Note: the Streaming API does not allow to stream on keyword AND location)
    filter_keywords = ['flood', 'flooding', 'flooded', 'inundation',
                       'inundate', 'swamp', 'tide']
    filter_langs = ['en']
    filter_location = [-7.64133, 50.10319, 1.75159, 60.15456]

    # start the stream based on location
    data_extractor.start_stream_location(filter_location)

    ################## 2. TRANSFORM DATA #########################

    # instantiate data_loader object
    data_loader = DataLoader(DATABASE_URI_TRIAL)

    # drop database if you only need the new data
    # data_loader.recreate_database()

    transformed_array = data_loader.transform_data(stream_listener)

    ################## 3. LOAD DATA #########################

    # load each tweet object to the database (PostgreSQL)
    for tweet_obj in transformed_array:
        # by calling start_load() method
        data_loader.start_load(tweet_obj)


# execute the script
if __name__ == "__main__":

    # by calling the main function
    main()
