import networkx as nx
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from elasticsearch import Elasticsearch
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
        return obj

    @classmethod
    def build_seed_graph(cls, name):
        obj = cls(name=name)
        print("Build the graph from the seed present in the table {}".format(obj.seed_data_table))
        query = "SELECT twitter_id FROM {0};"
        seeds = obj.session.execute(query.format(obj.seed_data_table))
        for seed in seeds:
            obj.seeds.append(int(seed.twitter_id))
        obj.graph.add_nodes_from(obj.seeds)
        for seed in obj.seeds:
            query = "SELECT friend_follower_id, is_friend, is_follower FROM {0} WHERE user_id={1}"
            statement = SimpleStatement(query.format(obj.network_table, seed), fetch_size=1000)
            for row in obj.session.execute(statement):
                if row.friend_follower_id in obj.seeds:
                    if row.is_friend:
                        obj.graph.add_edge(seed, row.friend_follower_id)
                    elif row.is_follower:
                        obj.graph.add_edge(row.friend_follower_id, seed)
        obj.modified_graph = obj.graph
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
            nx.write_gml(graph_to_save, DEFINITIONS_ROOT+"/data/graph/{0}/{1}.gml".format(self.name, self.name)
                         , stringizer=str)

    @staticmethod
    def pickle_loader(file_path):
        if not os.path.exists(file_path):
            print("Failed to load file at location: {} ".format(file_path))
            return False
        else:
            print("Load file: {}".format(file_path))
            return pickle.load(open(file_path, "rb"))

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
        self.graph = nx.DiGraph()
        self.modified_graph = self.graph
        self.seeds = []
        self.name = name
        if not os.path.exists(DEFINITIONS_ROOT+"/data/graph/{0}".format(name)):
            os.makedirs(DEFINITIONS_ROOT+"/data/graph/{0}".format(name))


"""
HELPERS FUNCTIONS
"""


def sort_node_by_importance(nodes_dictionnary):
    sorted_nodes = sorted(nodes_dictionnary.items(), key=lambda x: x[1], reverse=True)
    return sorted_nodes
