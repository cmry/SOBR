from pymongo import MongoClient
from bson.json_util import dumps, loads

#client = MongoClient("localhost", 27010)             
client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
db = client.research
#db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

group_authors_intermediate_pipeline = [
    {'$group': {'_id': '$author_id', 'labels': {'$mergeObjects': '$labels'}}},
    {'$project': {'author_id': '$_id', 'labels': 1, '_id': 0}},
]

result = list(db.labelled_authors_intermediate.aggregate(group_authors_intermediate_pipeline, allowDiskUse=True))

assert len(result) == len(set([x['author_id'] for x in result]))

# Replace the old collection with the new one
db.labelled_authors_intermediate.drop()
db.labelled_authors_intermediate.insert_many(result)