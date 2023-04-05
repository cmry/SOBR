from pymongo import MongoClient
from bson.json_util import dumps, loads
import time
import datetime

#client = MongoClient("localhost", 27010)             
client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
db = client.research
#db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

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

t0 = time.time()

# Mine the posts from the personality subreddits one personality at a time
for personality in personalities.keys():

    main_db_pipeline = [
        # Match the posts from the personality subreddits
        {'$match': {'subreddit_name_prefixed': {'$in': subreddits}}},

        # Match the posts by regex on flair 
        {'$match': {'author_flair_text': {'$regex': regex_dic[personality]}}},

        # Project the fields we need
        {'$project': {'author_fullname': 1, 'author_flair_text': 1, 'subreddit_name_prefixed': 1, 'post_id': '$id', '_id': 0}}, 

        # Group the posts by author
        {'$group': {'_id': '$author_fullname', personalities[personality]: {'$addToSet': {'post_id': '$post_id', 'flair': '$author_flair_text', 'subreddit_with_prefix': '$subreddit_name_prefixed', 'database_month': database_month}}}},

        # Format the output
        {'$addFields': {'personality': {personalities[personality]: '$' + personalities[personality]}}},
        {'$addFields': {'labels': {'personality': '$personality'}}},
        {'$project': {'author_id': '$_id', 'labels': 1, '_id': 0}},
    ]

    results = list(db.july2021_all.aggregate(main_db_pipeline, allowDiskUse=True))
    db.personality_temp.insert_many(results)

# Group all the posts by author
temp_db_pipeline = [
    # Expand the arrays to individual documents
    {'$unwind': {'path': '$labels.personality.extrovert', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.introvert', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.sensing', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.intuitive', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.thinking', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.feeling', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.judging', 'preserveNullAndEmptyArrays': True}},
    {'$unwind': {'path': '$labels.personality.perceiving', 'preserveNullAndEmptyArrays': True}},

    # Regroup the documents by author to form one document per author
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

    # Format the output
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
    
    # Exclude the authors that have contradictory labels. For example, introvert and extrovert
    {'$match': {'$nor': [{'$and': [{'labels.personality.extrovert.0': {'$exists': True}}, {'labels.personality.introvert.0': {'$exists': True}}]},
                        {'$and': [{'labels.personality.sensing.0': {'$exists': True}}, {'labels.personality.intuitive.0': {'$exists': True}}]},
                        {'$and': [{'labels.personality.thinking.0': {'$exists': True}}, {'labels.personality.feeling.0': {'$exists': True}}]},
                        {'$and': [{'labels.personality.judging.0': {'$exists': True}}, {'labels.personality.perceiving.0': {'$exists': True}}]},
                       ]
                }
    },
    {'$out': 'personality_temp'}
    
]
db.personality_temp.aggregate(temp_db_pipeline, allowDiskUse=True)

# Check that authors don't appear more than once
duplicate_authors_pipeline = [
    {'$group': {'_id': '$author_id', 'count': {'$sum': 1}}},
    {'$match': {'count': {'$gt': 1}}}
]
duplicate_authors = list(db.personality_temp.aggregate(duplicate_authors_pipeline, allowDiskUse=True))
assert len(duplicate_authors) == 0


# Check that there are all authors have labels
check_every_author_has_labels_pipeline = [
    {'$match': {'$and': [{'labels.personality.extrovert': {'$exists': False}},
                        {'labels.personality.introvert': {'$exists': False}},
                        {'labels.personality.sensing': {'$exists': False}},
                        {'labels.personality.intuitive': {'$exists': False}},
                        {'labels.personality.thinking': {'$exists': False}},
                        {'labels.personality.feeling': {'$exists': False}},
                        {'labels.personality.judging': {'$exists': False}},
                        {'labels.personality.perceiving': {'$exists': False}},
                        ]
                }
    }
]
authors_with_no_labels = list(db.personality_temp.aggregate(check_every_author_has_labels_pipeline, allowDiskUse=True))
assert len(authors_with_no_labels) == 0

# Check that no author has contradictory labels
check_contradictory_labels_pipeline = [
    {'$match': {'$or': [{'$and': [{'labels.personality.extrovert.0': {'$exists': True}}, {'labels.personality.introvert.0': {'$exists': True}}]},
                        {'$and': [{'labels.personality.sensing.0': {'$exists': True}}, {'labels.personality.intuitive.0': {'$exists': True}}]},
                        {'$and': [{'labels.personality.thinking.0': {'$exists': True}}, {'labels.personality.feeling.0': {'$exists': True}}]},
                        {'$and': [{'labels.personality.judging.0': {'$exists': True}}, {'labels.personality.perceiving.0': {'$exists': True}}]},
                        ] 
                }
    }
]
contradicting_authors = list(db.personality_temp.aggregate(check_contradictory_labels_pipeline, allowDiskUse=True))
assert len(contradicting_authors) == 0

authors = list(db.personality_temp.find({}, {'_id': 0}))
db.labelled_authors_intermediate.insert_many(authors)

db.personality_temp.drop()

elapsed = str(datetime.timedelta(seconds=int(round(time.time() - t0))))
print(f'Query took: {elapsed}')
    


