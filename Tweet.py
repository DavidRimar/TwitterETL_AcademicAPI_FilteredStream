from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.types import ARRAY, Float

Base = declarative_base()

"""
Tweet Class (extends Base class)
Representation of the data schema of 'Tweet' table in the PostgreSQL.
"""


class Tweet(Base):
    __tablename__ = 'filteredStreams'  # 'academicTweets'
    id = Column(Integer, primary_key=True)  # Auto-generated ID
    twitter_id = Column(String)  # (data.id)
    text = Column(String)  # (data.text)
    lang = Column(String)  # (data.lang)
    created_at = Column(DateTime)  # (data.created_at)
    places_geo_place_id = Column(String)  # (includes.places.id)
    places_geo_bbox = Column(ARRAY(Float))  # (includes.places.geo.bbox)
    places_full_name = Column(String)  # (includes.places.full_name)
    places_place_type = Column(String)  # (includes.places.place_type)
    places_country_code = Column(String)  # (includes.places.country_code)
    stream_rule_tag = Column(String)  # (matching_rules.tag)

    # Constructor
    def __repr__(self):
        return "<Tweet(twitter_id='{}', text='{}', lang={}, created_at={}, places_geo_place_id={}, places_geo_bbox={}, places_full_name={}, places_place_type={}, places_country_code={}, stream_rule_tag={})>"\
            .format(self.twitter_id, self.text, self.lang, self.created_at, self.places_geo_place_id, self.places_geo_bbox, self.places_full_name, self.places_place_type, self.places_country_code, self.stream_rule_tag)


"""
Example of a streamed response object in JSON format
{
    "data": [
        {
            "text": "We’re sharing a live demo of the new Twitter Developer Labs program, led by a member of our DevRel team, @jessicagarson #TapIntoTwitter https://t.co/ghv7f4dW5M",
            "id": "1136048014974423040",
            "geo": {
                "place_id": "01a9a39529b27f36"
            }
        }
    ],
    "includes": {
        "places": [
            {
                "geo": {
                    "type": "Feature",
                    "bbox": [
                        -74.026675,
                        40.683935,
                        -73.910408,
                        40.877483
                    ],
                    "properties": {}
                },
                "country_code": "US",
                "name": "Manhattan",
                "id": "01a9a39529b27f36",
                "place_type": "city",
                "country": "United States",
                "full_name": "Manhattan, NY"
            }
        ]
    }
"""
