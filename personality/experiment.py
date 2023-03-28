from pymongo import MongoClient
from bson.json_util import dumps, loads

# Connect to the MongoDB, change the connection string pmongodb accumulate from several documenter your MongoDB environment
client = MongoClient("localhost", 27010)             
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")


get_inconsistent_author_ids_pipeline = [
    {'$match': {'$or': [
    {'$and':[{'labels.personality.extrovert':{'$exists': True, '$ne': []}}, {'labels.personality.introvert':{'$exists': True, '$ne': []}}]},
    {'$and':[{'labels.personality.sensing':{'$exists': True, '$ne': []}}, {'labels.personality.intuitive':{'$exists': True, '$ne': []}}]},
    {'$and':[{'labels.personality.thinking':{'$exists': True, '$ne': []}}, {'labels.personality.feeling':{'$exists': True, '$ne': []}}]},
    {'$and':[{'labels.personality.judging':{'$exists': True, '$ne': []}}, {'labels.personality.perceiving':{'$exists': True, '$ne': []}}]},
    ]}},
    {'$group': {'_id': ' ', 'author_ids': {'$push': '$author_id'}}},
    {'$project': {'_id': 0, 'author_ids': 1}},
]
    
inconsistent_author_ids = list(db.labelled_authors.aggregate(get_inconsistent_author_ids_pipeline))[0]['author_ids']

print(f'Deleted {len(inconsistent_author_ids)} inconsistent authors with the following ids: {inconsistent_author_ids}')

db.labelled_authors.delete_many({'author_id': {'$in': inconsistent_author_ids}})

