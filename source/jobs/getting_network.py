import tweepy
from cassandra.cluster import Cluster

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from source.settings import settings
    import source.lib.network_library as nl


def cassandra_initialisation(session, strategy, replication_factor, keyspace, table_name):
    request = "CREATE KEYSPACE IF NOT EXISTS {0}" \
              " WITH replication = {{'class': '{1}', 'replication_factor': '{2}' }}"
    request = request.format(keyspace, strategy, replication_factor)
    session.execute(request)
    session.set_keyspace(keyspace)
    table = """CREATE TABLE IF NOT EXISTS {} (
                node_id bigint ,
                screen_name text,
                centrality float,
                friend_follower_id bigint,
                is_friend boolean,
                is_follower boolean,
                PRIMARY KEY ((node_id), friend_follower_id)
            );""".format(table_name)
    session.execute(table)


def elasticsearch_initialisation(es, es_index, index_settings, document_type, mapping):
    if not es.indices.exists(index=es_index):
        es.indices.create(index=es_index, body=index_settings)
    es.indices.put_mapping(index=es_index, doc_type=document_type, body=mapping)


def get_data(tweepy_function, author_id, author_username, session):
    try:
        if tweepy_function == "followers":
            followers = set()
            for follower_id in tweepy.Cursor(API.followers_ids, id=author_id, count=5000).items():
                if len(followers) % 5000 == 0 and len(followers) != 0:
                    print("Collected followers: ", len(followers))
                followers.add(follower_id)
                query = "INSERT INTO {0} (node_id, screen_name, friend_follower_id, centrality, is_follower) VALUES " \
                        "({1}, '{2}', {3}, {4}, true)".format("network", author_id, author_username, follower_id, 0.0)
                session.execute(query)
        if tweepy_function == "friends":
            friends = set()
            for friend_id in tweepy.Cursor(API.friends_ids, id=author_id, count=5000).items():
                if len(friends) % 5000 == 0 and len(friends) != 0:
                    print("Collected friends: ", len(friends))
                friends.add(friend_id)
                query = "INSERT INTO {0} (node_id, screen_name, friend_follower_id, centrality, is_friend) VALUES " \
                        "({1}, '{2}', {3}, {4}, true)".format("network", author_id, author_username, friend_id, 0.0)
                session.execute(query)
    except Exception as e:
        if "Not authorized." in str(e.response.content):
            pass
        else:
            sys.exit()


def seed_job():
    cluster = Cluster(['192.168.2.33'], port=9042)
    session = cluster.connect()
    query = "SELECT * FROM article.author"

    cassandra_initialisation(session, "SimpleStrategy", 2, "twitter", "network")

    authors = session.execute(query)
    methods = ["followers", "friends"]

    for author in authors:
        print(author.twitter_username)
        is_present = session.execute("SELECT * FROM {0} WHERE node_id={1}".format("network", author.twitter_id))
        if not is_present:
            for method in methods:
                get_data(method, author.twitter_id, author.twitter_username, session)
        else:
            print(author.twitter_username, "Already present in the database ")


def centrality_job(extended_seed_size_wanted):
    cluster = Cluster(['192.168.2.33'], port=9042)
    session = cluster.connect()

    cassandra_initialisation(session, "SimpleStrategy", 2, "twitter", "network")

    methods = ["followers", "friends"]
    graph = nl.SocialGraph.build_graph_from_seed("whole_graph")
    node_by_centrality_betweenness = graph.get_nodes_by_betweenness_centrality(5000)
    sorted_nodes_by_centrality = nl.sort_node_by_importance(node_by_centrality_betweenness)
    for node, centrality in sorted_nodes_by_centrality[:extended_seed_size_wanted]:
        print(node)
        is_present = session.execute("SELECT * FROM {0} WHERE node_id={1}".format("network", node))
        if not is_present:
            for method in methods:
                get_data(method, node, None, session)
        else:
            print("{} Already present in the database ".format(node))


AUTH = tweepy.OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
AUTH.set_access_token(settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)
API = tweepy.API(AUTH)


centrality_job(100)
