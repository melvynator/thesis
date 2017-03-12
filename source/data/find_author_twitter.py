from elasticsearch import Elasticsearch
from source.lib import author_library

es_host = {"host": '192.168.2.33'}
es = Elasticsearch([es_host])

documents = es.search(body={
    "query": {
        "query_string": {
            "query": "*"
        }
    },
    "aggs": {
        "tags": {
            "terms": {
                "size": 1000,
                "field": "author"
            }
        }
    }
}, index="article", doc_type=["dailystormer", "amren"], size=0)
authors = set()
for document in documents['aggregations']['tags']['buckets']:
    authors.add(document['key'])
print(len(authors))

dict_author = {}

for author in authors:
    if author == "AR Staff":
        author_object = author_library.Author.from_twitter(author, "@AmRenaissance")  # official
        print(author_object)
        author_object.save()
    elif author == "Pat Buchanan":
        author_object = author_library.Author.from_twitter(author, "@PatrickBuchanan")  # official
        print(author_object)
        author_object.save()
    elif author == "Zeiger":
        author_object = author_library.Author.from_twitter(author, "@PlaceInTheSun2")
        print(author_object)
        author_object.save()
    elif author == "Andrew Anglin":
        author_object = author_library.Author.from_twitter(author, "@FeministSilence")
        print(author_object)
        author_object.save()
    elif author == "morgoth":
        author_object = author_library.Author.from_twitter(author, "@morgoth_rev")  # official
        print(author_object)
        author_object.save()
    elif author == "Richard Spencer":
        author_object = author_library.Author.from_twitter(author, "@RichardBSpencer")  # official
        print(author_object)
        author_object.save()
    elif author == "Jared Taylor":
        author_object = author_library.Author.from_twitter(author, "@jartaylor")  # official
        print(author_object)
        author_object.save()
    elif author == "Fred Reed":
        author_object = author_library.Author.from_twitter(author, "@wcsoto")
        print(author_object)
        author_object.save()
    elif author == "azzmador":
        author_object = author_library.Author.from_twitter(author, "@rex_caerulus")
        print(author_object)
        author_object.save()
    elif author == "Sven Longshanks":
        author_object = author_library.Author.from_twitter(author, "@RadioAryan")  # official
        print(author_object)
        author_object.save()
    elif author == "David Adams":
        author_object = author_library.Author.from_twitter(author, "@dadamskatz1")  # official
        print(author_object)
        author_object.save()
    elif author == "Dan Roodt":
        author_object = author_library.Author.from_twitter(author, "@danroodt")  # official
        print(author_object)
        author_object.save()
    elif author == "Paul Kersey":
        author_object = author_library.Author.from_twitter(author, "@sbpdl")  # official
        print(author_object)
        author_object.save()
    elif author == "Christopher Green":
        author_object = author_library.Author.from_twitter(author, "@DefiantLionUK")  # official
        print(author_object)
        author_object.save()
    elif author == "John Derbyshire":
        author_object = author_library.Author.from_twitter(author, "@DissidentRight")  # official
        print(author_object)
        author_object.save()
    elif author == "Filip Dewinter":
        author_object = author_library.Author.from_twitter(author, "@FDW_VB")  # official
        print(author_object)
        author_object.save()
    elif author == "Henry Wolff":
        author_object = author_library.Author.from_twitter(author, "@TweetBrettMac")
        print(author_object)
        author_object.save()
    elif author == "Gavin McInnes":
        author_object = author_library.Author.from_twitter(author, "@Gavin_McInnes")  # official
        print(author_object)
        author_object.save()
    elif author == "Ann Coulter":
        author_object = author_library.Author.from_twitter(author, "@AnnCoulter")  # official
        print(author_object)
        author_object.save()

