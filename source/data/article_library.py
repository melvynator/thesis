import json
import urllib.request
import cassandra
import re
import datetime
import random
import abc
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
import plotly.offline as po
from plotly.graph_objs import *
import plotly.graph_objs as go


# This class is used when you want to crawl one website, it defined some of the basic fields
class Article(abc.ABC):
    strategy = "SimpleStrategy"
    replication_factor = 2
    cassandra_keyspace = "article"
    cluster = Cluster(['192.168.2.33'], port=9042)
    session = cluster.connect()
    es_host = {"host": "192.168.2.33", "port": 9200}
    es = Elasticsearch([es_host])
    es_index = "article"
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

    @abc.abstractproperty
    def website_url(self):
        pass

    @abc.abstractproperty
    def mapping(self):
        pass

    @abc.abstractproperty
    def document_type(self):
        pass

    # Constructor of the class, define all the fields
    # Take 1 parameter in input:
    #          - The class
    @abc.abstractmethod
    def __init__(self):
        pass

    # Create an index in elasticsearch if no one already exists
    # Take 0 parameter in input
    @abc.abstractmethod
    def elasticsearch_initialisation(self):
        pass

    # Create a keyspace in cassandra
    # Take 2 parameters in input:
    #          - The replication strategy
    #          - The replication factor
    @abc.abstractmethod
    def cassandra_initialisation(self, strategy, replication_factor):
        pass

    # Extract all the article from a piece of HTML
    # Take 1 parameter in input:
    #          - Piece of HTML contaning all the paragraphs
    @staticmethod
    @abc.abstractmethod
    def get_pure_article(full_article):
        pass

    # Extract all the word type by the author without the quote
    # Take 1 parameter in input:
    #          - Piece of HTML contaning all the paragraphs
    @staticmethod
    @abc.abstractmethod
    def get_author_wording(full_article):
        pass

    # Extract all the link present in an article
    # Take 1 parameter in input:
    #          - Piece of HTML contaning all the paragraphs
    @staticmethod
    @abc.abstractmethod
    def get_links(full_article):
        pass

    # Extract the domain name of all the link in the article
    # Take 1 parameter in input:
    #          - All the link present in the article
    @staticmethod
    @abc.abstractmethod
    def get_sources(links):
        pass

    @staticmethod
    @abc.abstractmethod
    # Extract the date from the header
    # Take 1 parameter in input:
    #          - The header containing the date
    def get_date(header):
        pass

    # Construct an object Article after crawling the data
    # Take 2 parameters in input:
    #          - The class
    #          - URL to crawl
    @classmethod
    @abc.abstractmethod
    def from_crawler(cls, url):
        pass

    # Construct an object Article from a elasticsearch document
    # Take 2 parameters in input:
    #          - The class
    #          - The elasticsearch document
    @classmethod
    @abc.abstractmethod
    def from_elasticsearch(cls, elasticsearch_article):
        pass

    # Construct an object Article from a cassandra row
    # Take 2 parameters in input:
    #          - The class
    #          - The cassandra row
    @classmethod
    @abc.abstractmethod
    def from_cassandra(cls, cassandra_row):
        pass

    # Recrawl the article based on its URL to update its field
    # Take 1 parameter in input:
    #          - The class
    @abc.abstractmethod
    def recrawl(self):
        pass

    # Insert an object into Cassandra database
    # Take 1 parameter in input:
    #          - The class
    @abc.abstractmethod
    def save_cassandra(self):
        pass

    # Insert an object into elasticsearch
    # Take 1 parameter in input:
    #          - The class
    @abc.abstractmethod
    def save_elasticsearch(self):
        pass

    # Update an object
    # Take 2 parameters in input:
    #          - The class
    #          - The query containing the field to update
    @abc.abstractmethod
    def update(self, body):
        pass

    # Save the Article in both Elasticsearch and Cassandra
    # Take 2 parameters in input:
    #          - The class
    #          - The query containing the field to update
    @abc.abstractmethod
    def save_article(self):
        pass

    # Check if an article is already present, return a boolean
    # Take 2 parameters in input:
    #          - The class
    #          - The URL to check
    @abc.abstractmethod
    def is_present(self, url):
        pass

    # Function to print the object Article
    # Take 1 parameters in input:
    #          - The class
    @abc.abstractmethod
    def __str__(self):
        pass

    # Function to represent the object Article
    # Take 1 parameters in input:
    #          - The class
    @abc.abstractmethod
    def __repr__(self):
        pass

# This class is used when you want to create a dailystormer object
class DailystormerArticle(Article):
    website_url = "http://www.dailystormer.com/"
    mapping = {
        "properties": {
            "title": {"type": "text", "fielddata": True},
            "author": {"type": "keyword"},
            "url": {"type": "keyword"},
            "article": {"type": "text", "fielddata": True},
            "author_wording": {"type": "text", "fielddata": True},
            "nb_comment": {"type": "integer"},
            "date": {
                "type": "date",
                "format": "yyyy-MM-dd"},
            "sources": {"type": "keyword"},
            "links": {"type": "keyword"},
        }
    }
    document_type = "dailystormer"

    def __init__(self):
        self.url = ""
        self.author = ""
        self.article = ""
        self.author_wording = ""
        self.article_id = ""
        year = random.randint(1900, 1910)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        self.date = datetime.datetime(year, month, day)
        self.title = ""
        self.links = []
        self.sources = []
        self.nb_comment = -1
        self.elasticsearch_initialisation()
        self.cassandra_initialisation(self.strategy, self.replication_factor)

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
                                article_id text,
                                title text,
                                author text,
                                date date,
                                article text,
                                author_wording text,
                                nb_comment int,
                                links list<text>,
                                sources list<text>)
                                WITH comment='Table of {1} article'""".format(self.document_type, self.document_type)
        self.session.execute(table)

    @staticmethod
    def get_pure_article(full_article):
        paragraphs = full_article.find_all("p")
        all_article = ""
        for paragraph in paragraphs[1:]:
            all_article += " ".join(paragraph.text.split())
        return all_article

    @staticmethod
    def get_author_wording(full_article):
        paragraphs = full_article.find_all("p")
        author_wording = ""
        for paragraph in paragraphs[1:]:
            if not paragraph.find_parent("blockquote") is None:
                author_wording += " ".join(paragraph.text.split())
        return author_wording

    @staticmethod
    def get_links(full_article):
        a_tags = full_article.find_all('a', href=True)
        i_frames = full_article.find_all('iframe', src=True)
        links = []
        for tag in a_tags:
            links.append(tag['href'])
        for i_frame in i_frames:
            links.append(i_frame['src'])
        return links

    @staticmethod
    def get_sources(links):
        sources = []
        for link in links:
            regex = re.search('.*://.*?(/|$)', link)
            if regex is not None:
                source = regex.group(0)
                if source[-1] == '/':
                    sources.append(source[:-1])
                else:
                    sources.append(source)
        return list(sources)

    @staticmethod
    def get_date(soup):
        date = soup.find("time", style=False)
        if not date:
            paragraphs = soup.find_all("p")
            match = re.search(r'\S+?\s\d{1,2},\s\d{4}', paragraphs[0].text)
            if not match:
                date = datetime.datetime.strptime('June 23, 1912', '%B %d, %Y').strftime("%Y-%m-%d")
            else:
                try:
                    date = datetime.datetime.strptime(match.group().strip(), '%B %d, %Y').date().strftime("%Y-%m-%d")
                except:
                    date = datetime.datetime.strptime('June 23, 1912', '%B %d, %Y').strftime("%Y-%m-%d")
        else:
            date = datetime.datetime.strptime(date.text.strip(), '%B %d, %Y').strftime("%Y-%m-%d")
        return date

    @classmethod
    def from_crawler(cls, url):
        obj = cls()
        obj.url = url
        opener = urllib.request.build_opener()
        opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
        response = opener.open(url)
        soup = BeautifulSoup(response, 'html.parser')
        article = soup.find("div", {"class": "entry"})
        paragraphs = article.find_all("p")
        if len(paragraphs) == 0:  # Case if the HTML is not well formated
            return obj
        obj.title = soup.find('h1').text.strip()
        if soup.find("a", {"rel": "author"}) is not None:
            obj.author = soup.find("a", {"rel": "author"}).text.strip()
        else:
            obj.author = "Anonymous"
        obj.date = obj.get_date(soup)
        full_article = soup.find("div", {"class": "entry"})
        obj.article = obj.get_pure_article(full_article)
        obj.author_wording = obj.get_author_wording(full_article)
        obj.links = obj.get_links(full_article)
        obj.sources = obj.get_sources(obj.links)
        nb_comment = soup.find("p", {"class": "more-replies"})
        if not nb_comment:
            obj.nb_comment = 0
        else:
            if len(re.findall('\d+', nb_comment.text)) > 0:
                obj.nb_comment = int(re.findall('\d+', nb_comment.text)[0]) + 5
            else:
                obj.nb_comment = 0
        return obj

    @classmethod
    def from_elasticsearch(cls, elasticsearch_article):
        obj = cls()
        document = elasticsearch_article['_source']
        obj.url = document['url']
        obj.author = document['author']
        obj.article = document['article']
        obj.author_wording = document['author_wording']
        obj.date = datetime.datetime.strptime(document['date'], "%Y-%m-%d").strftime("%Y-%m-%d")
        obj.title = document['title']
        obj.links = document['links']
        obj.sources = document['sources']
        obj.article_id = elasticsearch_article['_id']
        obj.nb_comment = document['nb_comment']
        return obj

    @classmethod
    def from_cassandra(cls, cassandra_row):
        obj = cls()
        obj.url = cassandra_row.url
        obj.author = cassandra_row.author
        obj.article = cassandra_row.article
        obj.author_wording = cassandra_row.author_wording
        obj.date = str(cassandra_row.date)
        obj.title = cassandra_row.title
        obj.links = cassandra_row.links
        obj.sources = cassandra_row.sources
        obj.article_id = cassandra_row.article_id
        obj.nb_comment = cassandra_row.nb_comment
        return obj

    def recrawl(self):
        article_id = self.article_id
        recrawled_article = self.from_crawler(self.url)
        recrawled_article.article_id = article_id
        return recrawled_article

    def save_cassandra(self):
        insert_query = """
            INSERT INTO {0} (article_id, url, title, author, date,
                        article, author_wording, links, sources, nb_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """.format(self.document_type)
        self.session.execute(insert_query, (self.article_id, self.url, self.title, self.author, self.date, self.article,
                                            self.author_wording, self.links, self.sources, self.nb_comment))

    def save_elasticsearch(self):
        body = {
            'url': self.url,
            'title': self.title,
            'author': self.author,
            'date': self.date,
            'article': self.article,
            'author_wording': self.author_wording,
            'links': self.links,
            'sources': self.sources,
            'nb_comment': self.nb_comment
        }
        if self.article_id == "":
            response = self.es.index(index=self.es_index, doc_type=self.document_type, body=body)
            self.article_id = response['_id']
        else:
            self.es.index(index=self.es_index, doc_type=self.document_type, body=body, id=self.article_id)

    def save_article(self):
        if self.title != "":
            self.save_elasticsearch()
            self.save_cassandra()

    def update(self, body):
        if self.title != "":
            self.es.update(index=self.es_index, doc_type=self.document_type, id=self.article_id, body=body)
            self.save_cassandra()

    def is_present(self, url):
        query = "SELECT COUNT(*) FROM {0} WHERE url='{1}'".format(self.document_type, url)
        response = self.session.execute(query)
        if response[0].count == 0:
            return False
        else:
            return True

    def __str__(self):
        article = "title: " + self.title
        article += "\nauthor: " + self.author
        article += "\ndate: " + self.date
        article += "\narticle: " + self.article
        article += "\nsources: " + str(set(self.sources))
        return article

    def __repr__(self):
        return self.title + " " + self.url


# This class is used when you want to create a amren object
class AmrenArticle(Article):
    website_url = "https://www.amren.com/"
    mapping = {
        "properties": {
            "title": {"type": "text", "fielddata": True},
            "author": {"type": "keyword"},
            "url": {"type": "keyword"},
            "article": {"type": "text", "fielddata": True},
            "author_wording": {"type": "text", "fielddata":True},
            "date": {
                "type": "date",
                "format": "yyyy-MM-dd"},
            "sources": {"type": "keyword"},
            "links": {"type": "keyword"},
        }
    }
    document_type = "amren"

    def __init__(self):
        self.url = ""
        self.author = ""
        self.article = ""
        self.author_wording = ""
        self.article_id = ""
        year = random.randint(1900, 1910)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        self.date = datetime.datetime(year, month, day)
        self.title = ""
        self.links = []
        self.sources = []
        self.elasticsearch_initialisation()
        self.cassandra_initialisation(self.strategy, self.replication_factor)

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
                                article_id text,
                                title text,
                                author text,
                                date date,
                                article text,
                                author_wording text,
                                links list<text>,
                                sources list<text>)
                                WITH comment='Table of {1} article'""".format(self.document_type, self.document_type)
        self.session.execute(table)

    @staticmethod
    def get_pure_article(full_article):
        paragraphs = full_article.find_all("p")
        all_article = ""
        for paragraph in paragraphs[1:]:
            all_article += " ".join(paragraph.text.split())
        return all_article

    @staticmethod
    def get_author_wording(full_article):
        paragraphs = full_article.find_all("p")
        author_wording = ""
        for paragraph in paragraphs[1:]:
            if paragraph.find_parent("blockquote") is None:
                author_wording += " ".join(paragraph.text.split())
        return author_wording

    @staticmethod
    def get_links(full_article):
        a_tags = full_article.find_all('a', href=True)
        i_frames = full_article.find_all('iframe', src=True)
        links = []
        for tag in a_tags:
            links.append(tag['href'])
        for i_frame in i_frames:
            links.append(i_frame['src'])
        return links

    @staticmethod
    def get_sources(links):
        sources = []
        for link in links:
            regex = re.search('.*://.*?(/|$)', link)
            if regex is not None:
                source = regex.group(0)
                if source[-1] == '/':
                    sources.append(source[:-1])
                else:
                    sources.append(source)
        return list(sources)

    @staticmethod
    def get_date(soup):
        tag_date = soup.find("span", {"class": "date"})
        if tag_date:
            match = re.search(r'\S+?\s\d{1,2},\s\d{4}', tag_date.text)
            if not match:
                date = datetime.datetime.strptime('June 23, 1912', '%B %d, %Y').date().strftime("%Y-%m-%d")
            else:
                date = datetime.datetime.strptime(match.group().strip(), '%B %d, %Y').date().strftime("%Y-%m-%d")
        else:
            tag_date = soup.find("p", {"class": "articleSource"})
            match = re.search(r'\S+?\s\d{1,2},\s\d{4}', tag_date.text)
            if not match:
                date = datetime.datetime.strptime('June 23, 1912', '%B %d, %Y').date().strftime("%Y-%m-%d")
            else:
                date = datetime.datetime.strptime(match.group().strip(), '%B %d, %Y').date().strftime("%Y-%m-%d")
        return date

    @classmethod
    def from_crawler(cls, url):
        obj = cls()
        obj.url = url
        opener = urllib.request.build_opener()
        opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
        response = opener.open(url)
        soup = BeautifulSoup(response, 'html.parser')
        obj.date = obj.get_date(soup)
        obj.title = soup.find("h1", {"class": "title"}).text
        profile = soup.find("div", {"class": "profile-image"})
        if not profile:
            print("Warning author not found.")
            obj.author = "Anonymous"
        else:
            obj.author = profile.find("img", {"class", "photo"})['alt']
        full_article = soup.find("div", {"class": "arColumn"})
        obj.article = obj.get_pure_article(full_article)
        obj.author_wording = obj.get_author_wording(full_article)
        obj.links = obj.get_links(full_article)
        obj.sources = obj.get_sources(obj.links)
        return obj

    @classmethod
    def from_elasticsearch(cls, elasticsearch_article):
        obj = cls()
        document = elasticsearch_article['_source']
        obj.url = document['url']
        obj.author = document['author']
        obj.article = document['article']
        obj.author_wording = document['author_wording']
        obj.date = datetime.datetime.strptime(document['date'], "%Y-%m-%d").strftime("%Y-%m-%d")
        obj.title = document['title']
        obj.links = document['links']
        obj.sources = document['sources']
        obj.article_id = elasticsearch_article['_id']
        return obj

    @classmethod
    def from_cassandra(cls, cassandra_row):
        obj = cls()
        obj.url = cassandra_row.url
        obj.author = cassandra_row.author
        obj.article = cassandra_row.article
        obj.author_wording = cassandra_row.author_wording
        obj.date = str(cassandra_row.date)
        obj.title = cassandra_row.title
        obj.links = cassandra_row.links
        obj.sources = cassandra_row.sources
        obj.article_id = cassandra_row.article_id
        return obj

    def recrawl(self):
        article_id = self.article_id
        recrawled_article = self.from_crawler(self.url)
        recrawled_article.article_id = article_id
        return recrawled_article

    def save_cassandra(self):
        insert_query = """
            INSERT INTO {0} (article_id, url, title, author, date,
                        article, author_wording, links, sources)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """.format(self.document_type)
        self.session.execute(insert_query, (self.article_id, self.url, self.title, self.author, self.date, self.article,
                                            self.author_wording, self.links, self.sources))

    def save_elasticsearch(self):
        body = {
            'url': self.url,
            'title': self.title,
            'author': self.author,
            'date': self.date,
            'article': self.article,
            'author_wording': self.author_wording,
            'links': self.links,
            'sources': self.sources
        }
        if self.article_id == "":
            response = self.es.index(index=self.es_index, doc_type=self.document_type, body=body)
            self.article_id = response['_id']
        else:
            self.es.index(index=self.es_index, doc_type=self.document_type, body=body, id=self.article_id)

    def save_article(self):
        if self.title != "":
            self.save_elasticsearch()
            self.save_cassandra()

    def update(self, body):
        if self.title != "":
            self.es.update(index=self.es_index, doc_type=self.document_type, id=self.article_id, body=body)
            self.save_cassandra()

    def is_present(self, url):
        query = "SELECT COUNT(*) FROM {0} WHERE url='{1}'".format(self.document_type, url)
        response = self.session.execute(query)
        if response[0].count == 0:
            return False
        else:
            return True

    def __str__(self):
        article = "title: " + self.title
        article += "\nauthor: " + self.author
        article += "\ndate: " + self.date
        article += "\narticle: " + self.article
        article += "\nsources: " + str(set(self.sources))
        return article

    def __repr__(self):
        return self.title + " " + self.url


# This is a beta class to display some visualization about the articles crawled
class ArticlesAnalyser:

    # Function that return list of the number of source per article
    # Take 1 parameter:
    #          - The class
    def get_nb_source_per_article(self):
        source_per_article = []
        for article in self.articles:
            source_per_article.append(len(set(article.sources)))
        return source_per_article

    # Function that return list of the number of comment per article
    # Take 1 parameter:
    #          - The class
    def get_nb_comment_per_article(self):
        nb_comment_per_article = []
        for article in self.articles:
            nb_comment_per_article.append(article.nb_comment)
        reversed(sorted(nb_comment_per_article))
        return nb_comment_per_article

    # Function that return the frequency of some domains into the articles
    # Take 1 parameter:
    #          - The class
    def get_source_frequency(self):
        sources_frequency = {}
        for article in self.articles:
            sources = set(article.sources)
            for source in sources:
                if source in sources_frequency:
                    sources_frequency[source] += 1
                else:
                    sources_frequency[source] = 1
        return list(reversed(sorted(list(sources_frequency.items()), key=lambda tup: tup[1])))

    # Function that give the frequency of a string field
    # Take 2 parameter:
    #          - The class
    #          - The field wanted
    def get_frequency(self, field):
        authors_frequency = {}
        for article in self.articles:
            author = getattr(article, field)
            if author in authors_frequency:
                authors_frequency[author] += 1
            else:
                authors_frequency[author] = 1
        return list(reversed(sorted(list(authors_frequency.items()), key=lambda tup: tup[1])))

    # Function that return the figure of a frequency bar chart sorted
    # Take 3 parameters:
    #          - The field containing the wanted distribution (Instance of Frequency parameters)
    #          - The nb of column to display
    #          - The title of the figure
    @staticmethod
    def get_distribution_frequency(field, nb_column, title):
        token = ["Value: " + str(token_distri[0]) if isinstance(token_distri[0], int) else token_distri[0] for token_distri
                 in field]
        frequency = [token_distri[1] for token_distri in field]
        distribution = [go.Bar(x=token[:nb_column], y=frequency[:nb_column])]
        layout = go.Layout(title=title)
        fig = go.Figure(data=distribution, layout=layout)
        return fig

    # Function that return a box plot for a given field
    # Take 2 parameters:
    #          - The field containing the wanted numbers
    #          - The nb of column to display
    @staticmethod
    def get_box_plot(field, title):
        trace = go.Box(y=field, opacity=0.90, name="Distribution")
        data = [trace]
        layout = go.Layout(title=title)
        fig = go.Figure(data=data, layout=layout)
        return fig

    # Function that return an histogram
    # Take 2 parameters:
    #          - The field containing the wanted distribution
    #          - The nb of column to display
    @staticmethod
    def get_distribution(field, title):
        trace = go.Histogram(x=field, opacity=0.90)
        data = [trace]
        layout = go.Layout(title=title, barmode='group')
        fig = go.Figure(data=data, layout=layout)
        return fig

    # Constructor of the class
    # Take 2 parameters in input:
    #          - The class
    #          - A list of Articles
    def __init__(self, articles):
        self.articles = articles

        self.nb_source_per_article = self.get_nb_source_per_article()
        self.sources_frequency = self.get_source_frequency()
        self.figure_sources_frequency = self.get_distribution_frequency(self.sources_frequency, 30,
                                                                        "Number of time an domain name is quoted")
        self.figure_distribution_nb_source = self.get_distribution(self.nb_source_per_article,
                                                                   "Distribution of the number of "
                                                                   "source per article")
        self.figure_box_plot_nb_source = self.get_box_plot(self.nb_source_per_article,
                                                           "Box plot of the source number")
        if articles[0].document_type != "amren":
            self.nb_comment_per_article = self.get_nb_comment_per_article()
            self.figure_distribution_nb_comment = self.get_distribution(self.nb_comment_per_article,
                                                                        "Distribution of the comment numbers")
            self.figure_box_plot_nb_comment = self.get_box_plot(self.nb_comment_per_article,
                                                                "Box plot of the comment numbers")

        self.authors_frequency = self.get_frequency("author")
        self.figure_authors_frequency = self.get_distribution_frequency(self.authors_frequency, 30,
                                                                        "Number of articles published by an author")

        self.dates_frequency = self.get_frequency("date")
        self.figure_dates_frequency = self.get_distribution_frequency(self.dates_frequency, 30,
                                                                      "Number of articles published per date")

