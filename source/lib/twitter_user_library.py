from datetime import datetime
from source.lib.network_library import SocialGraph
from source.lib.network_library import sort_node_by_importance
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class TwitterUser:

    cluster = Cluster(['192.168.2.33'], port=9042)
    session = cluster.connect()
    es_host = {"host": "192.168.2.33", "port": 9200}
    es = Elasticsearch([es_host])
    user_table = "twitter.twitter_user"

    def get_average_tweet_numbers_per_hour(self):
        request = {
            "query": {
                "bool": {
                    "must": [{"match": {"user.id": self.twitter_id}}]
                }
            },
            "sort": [
                {"created_at": {"order": "asc"}}
            ]
        }
        es_response = self.es.search(doc_type="tweet", index="tweets", body=request, size=10000)
        if es_response["hits"]["total"] > 1:
            tweets = es_response["hits"]["hits"]
            earliest_tweet = datetime.strptime(tweets[0]["_source"]["created_at"], "%a %b %d %X %z %Y")
            latest_tweet = datetime.strptime(tweets[-1]["_source"]["created_at"], "%a %b %d %X %z %Y")
            return round((latest_tweet - earliest_tweet).total_seconds()/3600/len(tweets))
        else:
            return 0

    def get_tweet_number(self):
        request = {
            "query": {
                "bool": {
                    "must": [{"match": {"user.id": self.twitter_id}}]
                }
            }
        }
        es_response = self.es.search(doc_type="tweet", index="tweets", body=request, size=0)
        return es_response["hits"]["total"]

    def get_the_most_retweeted_users(self, in_friend=True):
        request = {
            "query": {
                "bool": {
                    "must": [{"match": {"user.id": self.twitter_id}},
                             {"exists": {"field": "retweeted_status"}}
                             ]
                }
            }
        }
        es_response = self.es.search(doc_type="tweet", index="tweets", body=request, size=10000)
        favorite_friends = {}
        if es_response["hits"]["total"] != 0:
            tweets = es_response["hits"]["hits"]
            favorite_friends = {}
            for tweet in tweets:
                friend = tweet["_source"]["retweeted_status"]["user"]["id"]
                if in_friend:
                    if friend in self.friends:
                        if friend in favorite_friends:
                            favorite_friends[friend] += 1
                        else:
                            favorite_friends[friend] = 1
                else:
                    if friend in favorite_friends:
                        favorite_friends[friend] += 1
                    else:
                        favorite_friends[friend] = 1
        favorite_friends = sort_node_by_importance(favorite_friends)
        return favorite_friends

    def get_users_that_retweet_the_most(self, in_follower=True):
        request = {
            "query": {
                "bool": {
                    "must": [{"match": {"retweeted_status.user.id": self.twitter_id}},
                             {"exists": {"field": "retweeted_status"}}
                             ]
                }
            }
        }
        tweets = helpers.scan(self.es, query=request, index="tweets", doc_type="tweet", scroll="2m")
        favorite_followers = {}
        if tweets:
            for tweet in tweets:
                follower = tweet["_source"]["user"]["id"]
                if in_follower:
                    if follower in self.followers:
                        if follower in favorite_followers:
                            favorite_followers[follower] += 1
                        else:
                            favorite_followers[follower] = 1
                else:
                    if follower in favorite_followers:
                        favorite_followers[follower] += 1
                    else:
                        favorite_followers[follower] = 1
        favorite_followers = sort_node_by_importance(favorite_followers)
        return favorite_followers

    def get_user_info(self):
        prepared = self.session.prepare("SELECT * FROM {0} WHERE node_id=?".format(self.user_table))
        result_query = self.session.execute(prepared, [self.twitter_id])
        user_info = {}
        if result_query:
            user = result_query[0]
            if user.screen_name:
                user_info["screen_name"] = user.screen_name
            if user.lang:
                user_info["lang"] = user.lang
            if user.location:
                user_info["location"] = user.location
            if user.description:
                user_info["description"] = user.description
            if user.nb_followers:
                user_info["nb_followers"] = user.nb_followers
            if user.nb_friends:
                user_info["nb_friends"] = user.nb_friends
        return user_info

    def __init__(self, twitter_id, friends, followers):
        self.twitter_id = twitter_id
        self.friends = friends
        self.followers = followers


graph = SocialGraph("sub_graph_14000_nodes")
test = TwitterUser(196168350, graph.graph.successors(196168350), graph.graph.predecessors(196168350))
print(test.get_average_tweet_numbers_per_hour())
print(test.get_user_info())
