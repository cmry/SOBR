from pymongo import MongoClient
from bson.json_util import dumps, loads

# Connect to the MongoDB, change the connection string pmongodb accumulate from several documenter your MongoDB environment
#client = MongoClient("localhost", 27010)             
client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
db = client.research
#db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

db_month_name = 'july2021_all'

personality_subreddits = ['r/entj', 'r/enfp', 'r/enfj', 'r/intp', 'r/esfj', 'r/esfp', 'r/infp', 'r/intj', 'r/infj', 'r/isfj', 'r/entp', 'r/estp', 'r/estj', 'r/istj', 'r/isfp', 'r/istp']

# TODO: adapt the script to include the rest of the attributes

pipeline = [ 
    {'$lookup': {'from': db_month_name, 'localField': 'author_id', 'foreignField': 'author_fullname', 'as': 'all_author_posts'}},
    {'$unwind': {'path': '$all_author_posts', 'preserveNullAndEmptyArrays': True}},
    {'$project': {'_id': 0,
                'author_id': 1,
                'post_id': '$all_author_posts.id',
                'date_posted': '$all_author_posts.created_utc',
                'subreddit': '$all_author_posts.subreddit_name_prefixed',
                'post_body': '$all_author_posts.body',
                'extrovert': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.extrovert'}, 0]}, 'then': 0, 'else': 1}},
                'introvert': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.introvert'}, 0]}, 'then': 0, 'else': 1}},
                'sensing': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.sensing'}, 0]}, 'then': 0, 'else': 1}},
                'intuitive': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.intuitive'}, 0]}, 'then': 0, 'else': 1}},
                'thinking': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.thinking'}, 0]}, 'then': 0, 'else': 1}},
                'feeling': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.feeling'}, 0]}, 'then': 0, 'else': 1}},
                'judging': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.judging'}, 0]}, 'then': 0, 'else': 1}},
                'perceiving': {'$cond': {'if': {'$eq': [{'$size': '$labels.personality.perceiving'}, 0]}, 'then': 0, 'else': 1}},
                'personality_in_domain': {'$cond': {'if': {'$in': ['$all_author_posts.subreddit_name_prefixed', personality_subreddits]}, 'then': 1, 'else': 0}},
    }},
]


results = list(db.labelled_authors.aggregate(pipeline, allowDiskUse=True))

with open('temp.json', 'w') as f:
    f.write(dumps(results, indent=2))