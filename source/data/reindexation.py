from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import article_library


cluster = Cluster(['192.168.2.33'], port=9042)
session = cluster.connect()

query = "SELECT * FROM article.amren"

statement = SimpleStatement(query, fetch_size=100)
for article in session.execute(statement):
    ammren_article = article_library.AmrenArticle.from_cassandra(article)
    ammren_article.save_elasticsearch()

query = "SELECT * FROM article.dailystormer"

statement = SimpleStatement(query, fetch_size=100)
for article in session.execute(statement):
    dailystormer_article = article_library.DailystormerArticle.from_cassandra(article)
    dailystormer_article.save_elasticsearch()
