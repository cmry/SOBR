from pymongo import MongoClient
from bson.json_util import dumps, loads
from tqdm import tqdm
import time
import datetime

subreddits = ['r/europe', 'r/AskEurope', 'r/EuropeanCulture', 'r/EuropeanFederalists', 'r/Eurosceptics']

client = MongoClient("localhost", 27010)             
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

database_month = '07-2021'

pipeline = [
    {'$match': {'author_id': 't2_6l8tfjwf'}},
    {'$project': {'author_id': 1}},
    {'$addFields': {'labels': {'nationality': {'Romania': [{'post_id': 'o8q2x9', 'flair': 'Romania', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'}]}}}},
    {'$merge': {'into': 'labelled_authors_temp', 'on': 'author_id', 'whenMatched': 'merge', 'whenNotMatched': 'insert'}}
]

results = list(db.labelled_authors_temp.aggregate(pipeline))

with open('test.json', 'w') as f:
    f.write(dumps(results, indent=4))
