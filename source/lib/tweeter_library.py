from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
from elasticsearch import helpers
import tweepy
from cassandra.concurrent import execute_concurrent_with_args

from source.settings import settings

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

TWEETS_MAPPING = {
                    "template": "twitter*",
                    "settings": {
                        "number_of_shards": 1,
                        "index.mapping.total_fields.limit": 2000,
                    },
                    "mappings": {
                        "tweets": {
                            "properties": {
                                "coordinates": {
                                    "properties": {
                                        "coordinates": {
                                            "type": "geo_point"
                                        },
                                        "type": {
                                            "type": "string"
                                        },
                                    }
                                },
                                "text": {
                                    "type": "string"
                                },
                                "created_at": {
                                    "format": "EEE MMM dd HH:mm:ss Z yyyy",
                                    "type": "date"
                                },
                                "timestamp_ms": {
                                    "type": "date"
                                },
                                "user": {
                                    "properties": {
                                        "created_at": {
                                            "format": "EEE MMM dd HH:mm:ss Z yyyy",
                                            "type": "date"
                                        },
                                        "description": {
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

# Twitter
AUTH = tweepy.OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
AUTH.set_access_token(settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)
API = tweepy.API(AUTH)

# Cassandra
CLUSTER = Cluster(['192.168.2.33'], port=9042)
SESSION = CLUSTER.connect()

# Elasticsearch
ES_HOST = {"host": "192.168.2.33", "port": 9200}
ES = Elasticsearch([ES_HOST])


# Function to initialise cassandra manage the table and keyspace creation
# It will also set the KEYSPACE
def cassandra_initialisation(session, strategy, replication_factor, keyspace, table_name):
    request = "CREATE KEYSPACE IF NOT EXISTS {0}" \
              " WITH replication = {{'class': '{1}', 'replication_factor': '{2}' }}"
    request = request.format(keyspace, strategy, replication_factor)
    session.execute(request)
    session.set_keyspace(keyspace)
    table = """CREATE TABLE IF NOT EXISTS {} (
                node_id bigint,
                tweet_id bigint,
                json_tweet text,
                PRIMARY KEY ((node_id), tweet_id)
            );""".format(table_name)
    session.execute(table)


# Function that save the tweets in Cassandra and Elasticsearch
def save_tweets(node_id, tweets, session, es):
    save_tweets_in_cassandra(node_id, tweets, session)
    save_tweets_in_elasticsearch(tweets, es)


# Function to save the "protected user" that doesn't want to make their tweets public
def save_protected(node_id, session):
    prepared_statement = session.prepare("INSERT INTO tweets (node_id, tweet_id, json_tweet) VALUES ({}, ?, ?)"
                                         .format(node_id))
    session.execute(prepared_statement, (0, "protected"))


# Function that save the tweet into Cassandra
def save_tweets_in_cassandra(node_id, tweets, session):
    tweets_id = [tweet['id'] for tweet in tweets]
    tweet_string = [str(tweet) for tweet in tweets]
    prepared_statement = session.prepare("INSERT INTO tweets (node_id, tweet_id, json_tweet) VALUES ({}, ?, ?)"
                                         .format(node_id))
    execute_concurrent_with_args(session, prepared_statement, zip(tweets_id, tweet_string))
    print("Node: {0} containing {1} tweets save into the database".format(node_id, len(tweets)))


# Function that save the tweet into elasticsearch
def save_tweets_in_elasticsearch(tweets, es):
    for tweet in tweets:
        tweet['_id'] = tweet['id']
        tweet['_op_type'] = "index"
        tweet['_index'] = "tweets"
        tweet["_type"] = "tweet"
    helpers.bulk(actions=tweets, client=es, refresh=True)


# Access the tweet of a user via Tweepy and Twitter API
def get_tweet(node_id):
    tweets = []
    for tweet in tweepy.Cursor(API.user_timeline, id=node_id, count=200, include_rts=1,
                               wait_on_rate_limit=True, wait_on_rate_limit_notify=True).items():
        tweets.append(tweet)
    return tweets


# Function that process an ID and save the tweets corresponding to this user
def collect_and_save_tweet_from_user(user_id):
    query = "SELECT COUNT(*) FROM tweets WHERE node_id={0}".format(user_id)
    response = SESSION.execute(query)
    if response[0].count == 0:
        try:
            print("Getting node: {}".format(user_id))
            statuses = get_tweet(user_id)
            tweets = [status._json for status in statuses]
            save_tweets(user_id, tweets, SESSION, ES)
        except Exception:
            print("Protected user")
            save_protected(user_id, SESSION)
            pass
    else:
        print("Node: {0}, already fetch".format(user_id))


# Initialise Cassandra and elasticsearch
if not ES.indices.exists("tweets"):
    ES.indices.create("tweets", body=TWEETS_MAPPING)

cassandra_initialisation(SESSION, "SimpleStrategy", 2, "twitter", "tweets")



