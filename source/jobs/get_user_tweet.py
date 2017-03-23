from cassandra.query import SimpleStatement

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from source.lib import tweeter_library as tl
    from source.lib import network_library as nl


# Function that will retrieve ALL the last tweets of a user
def get_all_nodes_tweets():
    query = "SELECT friend_follower_id FROM network"
    statement = SimpleStatement(query, fetch_size=1000)
    for row in tl.SESSION.execute(statement):
        tl.collect_and_save_tweet_from_user(row.friend_follower_id)


# Function that will retrieve the tweets of users in a graph
def get_tweet_from_users_in_a_graph(graph):
    for node in graph.graph.nodes():
        tl.collect_and_save_tweet_from_user(node)


sub_graph_6000_nodes = nl.SocialGraph.build_graph_from_extended_seed_with_random_neighboors("sub_graph_6000_nodes",
                                                                                            graph_size=6000)
sub_graph_6000_nodes.export_gml()
get_tweet_from_users_in_a_graph(sub_graph_6000_nodes)
