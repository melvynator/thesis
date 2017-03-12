import urllib.request
import http.cookiejar
import random
import time
from bs4 import BeautifulSoup
from collections import Counter
import elasticsearch
from elasticsearch import Elasticsearch
import cassandra
from cassandra.cluster import Cluster


class Author:

    strategy = "SimpleStrategy"
    replication_factor = 2
    cassandra_keyspace = "article"
    cluster = Cluster(['192.168.2.33'], port=9042)
    session = cluster.connect()
    es_host = {"host": "192.168.2.33", "port": 9200}
    es = Elasticsearch([es_host])
    es_index = "author"
    document_type = "author"
    index_settings = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "analysis": {
                "analyzer": {
                    "article": {
                        "type": "standard",
                        "stopwords": "_english_",
                        "tokenizer": "standard"
                    }
                }
            }
        }
    }
    mapping = {
        "properties": {
            "author": {"type": "keyword"},
            "url": {"type": "keyword"},
            "twitter_id": {"type": "keyword"},
            "twiter_username": {"type": "keyword"},
        }
    }

    def elasticsearch_initialisation(self):
        if not self.es.indices.exists(index=self.es_index):
            self.es.indices.create(index=self.es_index, body=self.index_settings)
        self.es.indices.put_mapping(index=self.es_index, doc_type=self.document_type, body=self.mapping)

    def cassandra_initialisation(self, strategy, replication_factor):
        request = "CREATE KEYSPACE IF NOT EXISTS {0}" \
                  " WITH replication = {{'class': '{1}', 'replication_factor': '{2}' }}"
        request = request.format(self.cassandra_keyspace, strategy, replication_factor)
        self.session.execute(request)
        self.session.set_keyspace(self.cassandra_keyspace)
        table = """CREATE TABLE IF NOT EXISTS
                            {0} (
                                url text PRIMARY KEY,
                                author text,
                                twitter_id text,
                                twitter_username text,
                                )
                                WITH comment='Table of {1}'""".format(self.document_type, self.document_type)
        self.session.execute(table)

    def save_cassandra(self):
        insert_query = """
            INSERT INTO {0} (url, author, twitter_id, twitter_username)
            VALUES (%s, %s, %s, %s)
            """.format(self.document_type)
        self.session.execute(insert_query, (self.url, self.name, self.twitter_id, self.twitter_username))

    def save_elasticsearch(self):
        body = {
            'url': self.url,
            'author': self.name,
            'twitter_id': self.twitter_id,
            'twitter_username': self.twitter_username
        }
        self.es.index(index=self.es_index, doc_type=self.document_type, body=body)

    def save(self):
        self.save_elasticsearch()
        self.save_cassandra()

    @staticmethod
    def build_request(keyword):
        url = "https://twitter.com/search?q={0}&src=typd"
        query = keyword
        return url.format(query)

    @staticmethod
    def extract_result(ids_list, ats_list):
        ids = Counter(ids_list)
        ats = Counter(ats_list)
        ids_tuple = ids.most_common(1)
        ats_tuple = ats.most_common(1)
        if ids_tuple:
            most_common_id = ids_tuple[0][0]
            most_common_at = ats_tuple[0][0]
            nb_occurence_top_at = ats_tuple[0][1]
            if nb_occurence_top_at >= 5:
                author_result = {
                    "id": most_common_id,
                    "user_name": most_common_at.replace('/', '')
                }
            else:
                author_result = None
        else:
            return None
        return author_result

    @staticmethod
    def random_ua():
        list_ua = [
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/602.4.8 (KHTML, like Gecko) Version/10.0.3 Safari/602.4.8",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
            "Safari/12602.4.8 CFNetwork/807.2.14 Darwin/16.4.0 (x86_64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/602.4.8 (KHTML, like Gecko)"
        ]
        return random.choice(list_ua)


    @staticmethod
    def random_url():
        list_random_url = [

        ]

    def twitter_crawler(self, author):
        cookie_jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
        url = self.build_request(self.keyword)
        headers = [('Host', "twitter.com"),
                   ('User-Agent', self.random_ua()),
                   ('Accept', "application/json, text/javascript, */*; q=0.01"),
                   ('Accept-Language', "de,en-US;q=0.7,en;q=0.3"),
                   ('X-Requested-With', "XMLHttpRequest"),
                   ('Referer', url),
                   ('Connection', "keep-alive")
                   ]
        opener.addheaders = headers
        response = opener.open(url)
        html_response = response.read().decode('utf-8')
        soup = BeautifulSoup(html_response, 'html.parser')
        links = soup.find_all('a')
        div = soup.find('div', {"class":"ProfileCard js-actionable-user ProfileCard--wide "})
        if div:
                author_result = {
                    "id": div['data-user-id'],
                    "user_name": div['data-screen-name']
                }
                return author_result
        user_ids, user_ats = [], []
        for link in links:
            if 'user-id' in str(link) and 'mentioned' not in str(link):
                user_ids.append(link['data-user-id'])
                user_ats.append(link['href'])
        sleeper = random.uniform(3, 6)
        time.sleep(sleeper)
        return self.extract_result(user_ids, user_ats)

    def __init__(self, keyword):
        self.keyword = keyword
        self.elasticsearch_initialisation()
        self.cassandra_initialisation(self.strategy, self.replication_factor)
        self.name = ""
        self.twitter_id = ""
        self.twitter_username = ""
        self.url = ""

    @classmethod
    def from_twitter(cls, author, keyword):
        obj = cls(keyword)
        twitter_result = obj.twitter_crawler(author)
        obj.name = author
        if twitter_result:
            obj.twitter_username = twitter_result['user_name']
            obj.twitter_id = twitter_result['id']
            obj.url = "https://twitter.com/{0}".format(obj.twitter_username)
        else:
            obj.twitter_username = "NOT FOUND"
            obj.twitter_id = "NOT FOUND"
            obj.url = "NOT FOUND"
        return obj

    def __str__(self):
        string = "Author: " + self.name + "\n" + "username: " + self.twitter_username +\
                    "\n" + "id: " + self.twitter_id + "\n" + "URL: " + self.url
        return string

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.twitter_id == other.twitter_id

