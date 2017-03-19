import networkx as nx
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from elasticsearch import Elasticsearch
import random
import pickle
import os.path

from source.__init__ import DEFINITIONS_ROOT


class SocialGraph:
    cluster = Cluster(['192.168.2.33'], port=9042)
    session = cluster.connect()
    es_host = {"host": "192.168.2.33", "port": 9200}
    es = Elasticsearch([es_host])
    seed_data_table = "article.author"
    network_table = "twitter.network"

    @classmethod
    def build_graph_from_seed(cls, name):
        print("Building graph from seed")
        obj = cls(name=name)
        if len(obj.graph.nodes()) > 0:
            return obj
        else:
            query = "SELECT twitter_id FROM {0};"
            seeds = obj.session.execute(query.format(obj.seed_data_table))
            for seed in seeds:
                obj.seeds.append(int(seed.twitter_id))
                query = "SELECT friend_follower_id, is_friend, is_follower FROM {0} WHERE node_id={1}"
                statement = SimpleStatement(query.format(obj.network_table, seed.twitter_id), fetch_size=1000)
                friend_ids = []
                follower_ids = []
                for row in obj.session.execute(statement):
                    if row.is_friend:
                        friend_ids.append(row.friend_follower_id)
                    if row.is_follower:
                        follower_ids.append(row.friend_follower_id)
                if friend_ids:
                    for friend_id in friend_ids:
                        obj.graph.add_edge(int(seed.twitter_id), int(friend_id))
                if follower_ids:
                    for follower_id in follower_ids:
                        obj.graph.add_edge(int(follower_id), int(seed.twitter_id))
            obj.modified_graph = obj.graph
            obj.diffusers = obj.get_followed_nodes()
            return obj

    @classmethod
    def build_seed_graph(cls, name):
        obj = cls(name=name)
        if len(obj.graph.nodes()) > 0:
            return obj
        else:
            print("Build the graph from the seed present in the table {}".format(obj.seed_data_table))
            query = "SELECT twitter_id FROM {0};"
            seeds = obj.session.execute(query.format(obj.seed_data_table))
            for seed in seeds:
                obj.seeds.append(int(seed.twitter_id))
            obj.graph.add_nodes_from(obj.seeds)
            for seed in obj.seeds:
                query = "SELECT friend_follower_id, is_friend, is_follower FROM {0} WHERE node_id={1}"
                statement = SimpleStatement(query.format(obj.network_table, seed), fetch_size=1000)
                for row in obj.session.execute(statement):
                    if row.friend_follower_id in obj.seeds:
                        if row.is_friend:
                            obj.graph.add_edge(seed, row.friend_follower_id)
                        if row.is_follower:
                            obj.graph.add_edge(row.friend_follower_id, seed)
            obj.modified_graph = obj.graph
            obj.diffusers = obj.get_followed_nodes()
            return obj

    @classmethod
    def build_extended_seed_graph(cls, name):
        obj = cls(name=name)
        if len(obj.graph.nodes()) > 0:
            return obj
        else:
            print("Build the graph from the extended seed present in the table {}".format(obj.network_table))
            query = "SELECT DISTINCT node_id FROM {0};"
            seeds = obj.session.execute(query.format(obj.network_table))
            for seed in seeds:
                obj.seeds.append(int(seed.node_id))
            obj.graph.add_nodes_from(obj.seeds)
            for seed in obj.seeds:
                query = "SELECT friend_follower_id, is_friend, is_follower FROM {0} WHERE node_id={1}"
                statement = SimpleStatement(query.format(obj.network_table, seed), fetch_size=1000)
                for row in obj.session.execute(statement):
                    if row.friend_follower_id in obj.seeds:
                        if row.is_friend:
                            obj.graph.add_edge(seed, row.friend_follower_id)
                        if row.is_follower:
                            obj.graph.add_edge(row.friend_follower_id, seed)
            obj.modified_graph = obj.graph
            obj.diffusers = obj.get_followed_nodes()
            return obj

    @classmethod
    def build_graph_from_extended_seed_with_random_neighboors(cls, name, graph_size=2000):
        obj = cls(name=name)
        if len(obj.graph.nodes()) > 0:
            return obj
        else:
            print("Build the graph from extended seed with {0} random neighboors".format(graph_size))
            statement = SimpleStatement("SELECT DISTINCT node_id FROM {0}".format(obj.network_table))
            for node in obj.session.execute(statement):
                obj.seeds.append(node.node_id)
            obj.graph.add_nodes_from(obj.seeds)
            nodes_dictionnary = {}
            node_numbers = 0
            for seed in obj.seeds:
                query = "SELECT friend_follower_id, is_follower, is_friend FROM {0} WHERE node_id={1}"\
                    .format(obj.network_table, seed)
                statement = SimpleStatement(query, fetch_size=10000)
                friends, followers = [], []
                for row in obj.session.execute(statement):
                    if row.is_follower:
                        followers.append(row.friend_follower_id)
                    if row.is_friend:
                        friends.append(row.friend_follower_id)
                nodes_dictionnary[seed] = {}
                nodes_dictionnary[seed]["friends"] = friends
                nodes_dictionnary[seed]["followers"] = followers
                node_numbers += len(friends) + len(followers) + 1  # + 1 for the seed
            percentage_of_the_graph = float(100.0 * graph_size / node_numbers)
            print("The random graph will have {0}% of the initial graph ({1} nodes)"
                  .format(percentage_of_the_graph, node_numbers))
            probability_node = percentage_of_the_graph/100
            for seed, neighboors in nodes_dictionnary.items():
                if "friends" in neighboors:
                    seed_links = [seed for seed in obj.seeds if seed in neighboors["friends"]]
                    filter(lambda x: random.random() < probability_node, neighboors["friends"])
                    random_friends = [x for x in neighboors["friends"] if random.random() < probability_node]
                    random_friends += seed_links
                    for friend in random_friends:
                        obj.graph.add_edge(seed, friend)
                if "followers" in neighboors:
                    seed_links = [seed for seed in obj.seeds if seed in neighboors["followers"]]
                    filter(lambda x: random.random() < probability_node, neighboors["followers"])
                    random_followers = [x for x in neighboors["followers"] if random.random() < probability_node]
                    random_followers += seed_links
                    for followers in random_followers:
                        obj.graph.add_edge(followers, seed)
            print("Final number of nodes is: {0}, with {1} edges".format(len(obj.graph.nodes()),
                                                                         len(obj.graph.edges())))
            obj.modified_graph = obj.graph
            obj.diffusers = obj.get_followed_nodes()
            return obj

    def remove_useless_leaves(self, nb_out_edge=0):
        for node in self.modified_graph.nodes():
            if self.modified_graph.out_degree(node) == 1 and self.modified_graph.in_degree(node) == nb_out_edge:
                self.modified_graph.remove_node(node)

    def export_gml(self, path=None, g='modified'):
        graph_to_save = nx.DiGraph()
        if g == 'modified':
            graph_to_save = self.modified_graph
        if g == 'original':
            graph_to_save = self.graph
        if path:
            print("Save graph to location: {}".format(DEFINITIONS_ROOT+path))
            nx.write_gml(graph_to_save, DEFINITIONS_ROOT+path, stringizer=str)
        else:
            print("Save graph to lacation: {0}/data/graph/{1})".format(DEFINITIONS_ROOT, self.name))
            nx.write_gml(graph_to_save, DEFINITIONS_ROOT+"/data/graph/{0}/{1}.gml".format(self.name, self.name),
                         stringizer=str)

    # Get the list of all the user follow by at least one person in the graph
    def get_followed_nodes(self):
        followed_nodes = []
        for node in self.graph.nodes():
            if self.graph.in_degree(node) > 0:
                followed_nodes.append(node)
        print("We found {} user that have at least one follower in the graph".format(len(followed_nodes)))
        return followed_nodes

    # Search tweets written by the input user in Elasticsearch
    def retrieve_tweet_of_a_user(self, node):
        request = {
            "query": {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "retweeted_status"
                        }
                    },
                    "must": {
                        "match": {
                            "user.id": node
                        }
                    }
                }
            }
        }
        es_response = self.es.search(doc_type="tweet", index="tweets", body=request, size=3200)
        if es_response["hits"]["total"] != 0:
            tweets = es_response["hits"]["hits"]
            print("Found {0} tweets for the user: {1}".format(len(tweets), node))
            return tweets
        else:
            return []

    def retrieve_retweet_from_the_friends_of_a_user(self, node):
        all_retweet = []
        for friend in self.modified_graph.successors(node):
            request = {
                "query": {
                    "bool": {
                        "must": [{"match": {"user.id": node}},
                                 {"exists": {"field": "retweeted_status"}},
                                 {"match": {"retweeted_status.user.id": friend}},
                                 {"exists": {"field": "retweeted_status.text"}},
                                 {"wildcard": {"retweeted_status.entities.urls.display_url": "*amren*"}}
                                 ]
                    }
                }
            }
            es_response = self.es.search(doc_type="tweet", index="tweets", body=request, size=3200)
            if es_response["hits"]["total"] != 0:
                friend_retweet = es_response["hits"]["hits"]
                all_retweet += friend_retweet
        if all_retweet:
            print("The user: {0} retweet: {1} tweet(s) from his friends".format(node, len(all_retweet)))
        return all_retweet

    @staticmethod
    def pickle_loader(file_path):
        if not os.path.exists(file_path):
            print("Failed to load file at location: {} ".format(file_path))
            return False
        else:
            print("Load file: {}".format(file_path))
            return pickle.load(open(file_path, "rb"))

    @staticmethod
    def get_graph_from_gml(name):
        directory_path = DEFINITIONS_ROOT+"/data/graph/{0}/{1}.gml".format(name, name)
        if os.path.exists(directory_path):
            print("Load graph from file: {0}".format(directory_path))
            loaded_graph = nx.read_gml(directory_path)
            return loaded_graph
        else:
            return False

    def get_nodes_by_betweenness_centrality(self, nb_node, normalized=True):
        print("Trying to load the betweenness centrality from file...")
        file_path = DEFINITIONS_ROOT+"/data/graph/{0}/{1}_betweenness_centrality.p".format(self.name, str(nb_node))
        betweenness_centrality = self.pickle_loader(file_path)
        if not betweenness_centrality:
            print("Failed to load an existent betweenness centrality")
            print("Computing the betweenness centrality")
            betweenness_centrality = nx.algorithms.betweenness_centrality(
                self.graph, k=nb_node, normalized=normalized)
            pickle.dump(betweenness_centrality, open(file_path, "wb"))
        else:
            print("Load betweenness centrality from file")
        return betweenness_centrality

    def __init__(self, name):
        loaded_graph = self.get_graph_from_gml(name)
        if loaded_graph:
            self.graph = loaded_graph
            self.modified_graph = loaded_graph
            self.seeds = []
            self.diffusers = self.get_followed_nodes()
            self.name = name
        else:
            self.graph = nx.DiGraph()
            self.modified_graph = self.graph
            self.seeds = []
            self.name = name
            if not os.path.exists(DEFINITIONS_ROOT+"/data/graph/{0}".format(name)):
                os.makedirs(DEFINITIONS_ROOT+"/data/graph/{0}".format(name))


class TweetGraph:

    def __init__(self, social_graph):
        self.social_graph = social_graph
        original_diffusers_tweets = {}
        retweet_from_followers = {}
        for node in social_graph.get_followed_nodes():
            original_diffusers_tweets[node] = social_graph.retrieve_tweet_of_a_user(node)
        for node in social_graph.graph.nodes():
            retweet_from_followers[node] = social_graph.retrieve_retweet_from_friends()

"""
HELPERS FUNCTIONS
"""


def sort_node_by_importance(nodes_dictionnary):
    sorted_nodes = sorted(nodes_dictionnary.items(), key=lambda x: x[1], reverse=True)
    return sorted_nodes

#graph = SocialGraph.build_graph_from_extended_seed_with_random_neighboors("sub_graph_2000_nodes", graph_size=2000)
#i = 0
#for node in graph.graph.nodes():
#    if graph.retrieve_retweet_from_the_friends_of_a_user(node):
#        i += 1
#print(i)