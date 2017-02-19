import article_library
import cassandra
import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from elasticsearch import Elasticsearch
from elasticsearch import helpers

query = "SELECT * FROM article.dailystormer"
cluster = Cluster(['192.168.2.33'], port=9042)
session = cluster.connect()
es_host = {"host": "localhost", "port": 9200}
es = Elasticsearch([es_host])

t0= time.time()
# Cassandra
articles = []
statement = SimpleStatement(query, fetch_size=100)
for article in session.execute(statement):
    dailystormer_article = article_library.DailystormerArticle.from_cassandra(article)
    articles.append(dailystormer_article)

print("Read all in cassandra: ", time.time() - t0)


# Elasticsearch
t0= time.time()
documents = helpers.scan(es, query={ "query" : {"match_all" : {}}},index="article", doc_type="dailystormer",scroll="2m")
articles = []
for document in documents:
    dailystormer_article = article_library.DailystormerArticle.from_elasticsearch(document)
    articles.append(dailystormer_article)

print("Read all in elasticsearch: ", time.time() - t0)
