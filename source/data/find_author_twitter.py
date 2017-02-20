import tweepy
import re
import article_library
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import urllib.request
from bs4 import BeautifulSoup
from google import search


if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from settings import settings

es_host = {"host": '192.168.2.33'}
es = Elasticsearch([es_host])

documents = es.search(body={
    "query": {
        "query_string": {
            "query": "*"
        }
    },
    "aggs": {
        "tags": {
            "terms": {
                "size": 1000,
                "field": "author"
            }
        }
    }
}, index="article", doc_type=["dailystormer", "amren"], size=0)
authors = set()
for document in documents['aggregations']['tags']['buckets']:
    authors.add(document['key'])

auth = tweepy.OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
auth.set_access_token(settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

author_twitter = {}
for author in authors:
    author_to_query = author.replace(' ', '+')
    search_query = '{0} race site:twitter.com'.format(author_to_query)
    twitter_names = None
    for result in search(search_query, num=3, stop=1, pause=2, user_agent="Mozilla/5.0 (X11; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0"):
        print(author)
        split_url = result.split('/')
        if len(split_url) > 4 and split_url[4] == 'status':
            twitter_name = split_url[3]




#user = api.get_user('Andrew Langlin')
#print(user)
