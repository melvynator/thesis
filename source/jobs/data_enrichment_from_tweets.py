from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from elasticsearch import Elasticsearch
import json

ES_HOST = {"host": "192.168.2.33", "port": 9200}
ES = Elasticsearch([ES_HOST])

CLUSTER = Cluster(['192.168.2.33'], port=9042)
SESSION = CLUSTER.connect()
SESSION.set_keyspace("twitter")
TABLE_NAME = "twitter_user"


def create_new_cassandra_table(table_name):
    table = """CREATE TABLE IF NOT EXISTS
                                {0} (
                                    node_id bigint,
                                    screen_name text,
                                    description text,
                                    location text,
                                    lang text,
                                    nb_followers int,
                                    nb_friends int,
                                    PRIMARY KEY ((node_id), screen_name)
                                    )
                                    WITH comment='Data about every twitter user collected'""".format(table_name)
    SESSION.execute(table)


def get_all_ids():
    query = "SELECT node_id, friend_follower_id FROM network"
    statement = SimpleStatement(query, fetch_size=10000)
    ids = set()
    for row in SESSION.execute(statement):
        ids.add(row.friend_follower_id)
        ids.add(row.node_id)
    return ids


def build_query(twitter_id, field):
    user_info = {
        "query": {
            "bool": {
                "must": [{"match": {"user.id": twitter_id}},
                         {"exists": {"field": "user"}}
                         ]
            }
        }
    }
    retweet_info = {
        "query": {
            "bool": {
                "must": [{"match": {"retweeted_status.user.id": twitter_id}},
                         {"exists": {"field": "retweeted_status"}}
                         ]
            }
        }
    }
    entities_info = {
        "query": {
            "bool": {
                "must": [{"match": {"entities.user_mentions.id": twitter_id}},
                         {"exists": {"field": "entities.user_mentions"}}
                         ]
            }
        }
    }
    if field == "user":
        return user_info
    if field == "retweet":
        return retweet_info
    if field == "entities":
        return entities_info


def retrieve_document_given_a_body(body):
    es_response = ES.search(doc_type="tweet", index="tweets", body=body, size=1)
    if es_response["hits"]["total"] != 0:
        return es_response["hits"]["hits"][0]
    else:
        return False


def extract(document, twitter_id, field):
    if field == "user":
        user_info = document["_source"]["user"]
    elif field == "retweet":
        user_info = document["_source"]["retweeted_status"]["user"]
    else:
        users = document["_source"]["entities"]["user_mentions"]
        for user in users:
            if user["id"] == twitter_id:
                user_info = user
    data = {}
    if "screen_name" in user_info:
        data["screen_name"] = user_info["screen_name"]
    if "friends_count" in user_info:
        data["nb_friends"] = user_info["friends_count"]
    if "followers_count" in user_info:
        data["nb_followers"] = user_info["followers_count"]
    if "lang" in user_info:
        data["lang"] = user_info["lang"]
    if "location" in user_info:
        data["location"] = user_info["location"]
    if "description" in user_info:
        if user_info["description"] != "":
            data["description"] = user_info["description"]
    data["node_id"] = twitter_id
    return data


def save_user_info(data):
    prepared = SESSION.prepare('INSERT INTO {0} JSON ?'.format(TABLE_NAME))
    SESSION.execute(prepared, [json.dumps(data)])


def get_and_save_info(twitter_id):
    source_fields = ["user", "retweet", "entities"]
    for field in source_fields:
        query = build_query(twitter_id, field)
        result = retrieve_document_given_a_body(query)
        if result:
            info = extract(result, twitter_id, field)
            save_user_info(info)
            return True
    print("No information found for: {0}".format(twitter_id))


def main():
    create_new_cassandra_table(TABLE_NAME)
    ids = get_all_ids()
    for twitter_id in ids:
        get_and_save_info(twitter_id)


main()
