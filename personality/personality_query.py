from pymongo import MongoClient
from bson.json_util import dumps, loads

# Connect to the MongoDB, change the connection string per your MongoDB environment
client = MongoClient("localhost", 27010)             
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

database_month = '07-2021'

personalities = {'E': 'extrovert',
                    'I': 'introvert',
                    'S': 'sensing',
                    'N': 'intuitive',
                    'T': 'thinking',
                    'F': 'feeling',
                    'J': 'judging',
                    'P': 'perceiving'
                    }

subreddits = ['r/entj', 'r/enfp', 'r/enfj', 'r/intp', 'r/esfj', 'r/esfp', 'r/infp', 'r/intj', 'r/infj', 'r/isfj', 'r/entp', 'r/estp', 'r/estj', 'r/istj', 'r/isfp', 'r/istp']

regex_dic = {'E': '(^|\s)[Ee][SsNn][TtFf][PpJj](\s|$)',
             'I': '(^|\s)[Ii][SsNn][TtFf][PpJj](\s|$)',
             'S': '(^|\s)[EeIi][Ss][TtFf][PpJj](\s|$)',
             'N': '(^|\s)[EeIi][Nn][TtFf][PpJj](\s|$)',
             'T': '(^|\s)[EeIi][SsNn][Tt][PpJj](\s|$)',
             'F': '(^|\s)[EeIi][SsNn][Ff][PpJj](\s|$)',
             'J': '(^|\s)[EeIi][SsNn][TtFf][Jj](\s|$)',
             'P': '(^|\s)[EeIi][SsNn][TtFf][Pp](\s|$)'
             }

for personality in personalities.keys():

    main_db_pipeline = [
        # Match the posts from the personality subreddits
        {'$match': {'subreddit_name_prefixed': {'$in': subreddits}}},

        # Match the posts with the personality flair 
        {'$match': {'author_flair_text': {'$regex': regex_dic[personality]}}},

        # Project the fields we need
        {'$project': {'author_fullname': 1, 'author_flair_text': 1, 'subreddit_name_prefixed': 1, 'post_id': '$id', '_id': 0}}, 

        # Group the posts by author
        {'$group': {'_id': '$author_fullname', personalities[personality]: {'$addToSet': {'post_id': '$post_id', 'flair': '$author_flair_text', 'subreddit_with_prefix': '$subreddit_name_prefixed', 'database_month': database_month}}}},

        # Format the output
        {'$addFields': {'personality': {personalities[personality]: '$' + personalities[personality]}}},
        {'$addFields': {'labels': {'personality': '$personality'}}},
        {'$project': {'author_id': '$_id', 'labels': 1, '_id': 0}},
        {'$out': 'personality_temp'}
    ]

    db.july2021_all.aggregate(main_db_pipeline, allowDiskUse=True)
    
group_authors_pipeline = [
    {'$unwind': {'path': '$labels.personality.extrovert', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.introvert', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.sensing', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.intuitive', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.thinking', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.feeling', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.judging', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.perceiving', 'preserveNullAndEmptyArrays': True}},
    {'$group': {'_id': '$author_id',
                'extrovert': {'$push': '$labels.personality.extrovert'},
                'introvert': {'$push': '$labels.personality.introvert'},
                'sensing': {'$push': '$labels.personality.sensing'},
                'intuitive': {'$push': '$labels.personality.intuitive'},
                'thinking': {'$push': '$labels.personality.thinking'},
                'feeling': {'$push': '$labels.personality.feeling'},
                'judging': {'$push': '$labels.personality.judging'},
                'perceiving': {'$push': '$labels.personality.perceiving'},
                }},
    {'$addFields': {'personality': {'extrovert': '$extrovert',
                                    'introvert': '$introvert',
                                    'sensing': '$sensing',
                                    'intuitive': '$intuitive',
                                    'thinking': '$thinking',
                                    'feeling': '$feeling',
                                    'judging': '$judging',
                                    'perceiving': '$perceiving',
                                    }}},
    {'$addFields': {'labels': {'personality': '$personality'}}},
    {'$project':{'author_id': '$_id', 'labels': 1, '_id': 0}},
    {'$out': 'labelled_authors'}
]

db.labelled_authors_temp.aggregate(group_authors_pipeline, allowDiskUse=True)
db.labelled_authors_temp.drop()

    


