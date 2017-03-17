from cassandra.query import SimpleStatement
from ..lib import tweeter_library as tl


# Function that will retrieve ALL the last tweets of a user
def get_all_nodes_tweets():
    query = "SELECT friend_follower_id FROM network"
    statement = SimpleStatement(query, fetch_size=1000)
    for row in tl.SESSION.execute(statement):
        tl.collect_and_save_tweet_from_user(row.friend_follower_id)

get_all_nodes_tweets()