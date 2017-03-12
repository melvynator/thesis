import networkx as nx
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt

from source import DEFINITIONS_ROOT


class SocialGraph:

    cluster = Cluster(['192.168.2.33'], port=9042)
    session = cluster.connect()
    es_host = {"host": "192.168.2.33", "port": 9200}
    es = Elasticsearch([es_host])
    seed_data_table = "article.author"
    network_table = "twitter.network"

    @classmethod
    def build_graph_from_seed(cls, name):
        obj = cls()
        obj.name = name
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
                    obj.graph.add_edge(seed.twitter_id, friend_id)
            if follower_ids:
                for follower_id in follower_ids:
                    obj.graph.add_edge(follower_id, seed.twitter_id)
        obj.modified_graph = obj.graph
        return obj

    @classmethod
    def build_seed_graph(cls, name):
        obj = cls()
        obj.name = name
        query = "SELECT twitter_id FROM {0};"
        seeds = obj.session.execute(query.format(obj.seed_data_table))
        for seed in seeds:
            obj.seeds.append(int(seed.twitter_id))
        obj.graph.add_nodes_from(obj.seeds)
        for seed in obj.seeds:
            query = "SELECT friend_follower_id, is_friend, is_follower FROM {0} WHERE node_id={1}"
            statement = SimpleStatement(query.format(obj.network_table, seed), fetch_size=1000)
            friend_ids = []
            follower_ids = []
            for row in obj.session.execute(statement):
                if row.friend_follower_id in obj.seeds:
                    if row.is_friend:
                        friend_ids.append(row.friend_follower_id)
                    if row.is_follower:
                        follower_ids.append(row.friend_follower_id)
            if friend_ids:
                for friend_id in friend_ids:
                    obj.graph.add_edge(seed, friend_id)
            if follower_ids:
                for follower_id in follower_ids:
                    obj.graph.add_edge(follower_id, seed)
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
            nx.write_gml(graph_to_save, DEFINITIONS_ROOT+path, stringizer=str)
        else:
            nx.write_gml(graph_to_save, DEFINITIONS_ROOT+"/data/graph/{}.gml".format(self.name), stringizer=str)

    def __init__(self):
        self.graph = nx.DiGraph()
        self.modified_graph = self.graph
        self.seeds = []
        self.name = ""



print("Building the graph...")
graph = SocialGraph.build_seed_graph("manual_seed")
print(nx.dag_longest_path(graph.graph))
# print("Removing leaves...")
# graph.remove_useless_leaves()
# print("Computing the betweenness centrality")
#betweenness_centrality = nx.algorithms.betweenness_centrality(graph.modified_graph, k=400)
# print(betweenness_centrality)
# print("Saving the graph: {} to source/data/graph".format(graph.name))
# graph.export_gml(g='original')
# print("Graph exported!")
