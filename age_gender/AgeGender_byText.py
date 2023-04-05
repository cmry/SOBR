from pymongo import MongoClient
import pandas as pd
from bson.json_util import dumps, loads
import re
from tqdm import tqdm

#client = MongoClient("localhost", 27010)
client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
db = client.research
#db.authenticate("marilu", "topsecretpasswordformarilusmongo")

def query_mongoDB(patterns, regex_dic, posts_dic):
    '''
    A function to query a mongoDB collection selecting only those documents which text attribute contains the text of interest
    patterns: a list with the names of the different patterns (this names are the same of the dictionaries keys)
    regex_dic: a dictionary that link every pattern name to the actual pattern to be searched with regex
    db_file: mongoDB collection
    posts_dic: dictionary that will contain the objects queried (keys are the patterns)
    database_month: label to indicate the mongoBB collection (when saving)
    text_attribute: mongoDB attribute to query'''
    for p in tqdm(patterns, total = len(patterns)):

        query = [
            {'$match': {'body': {'$regex': regex_dic[p]}}},
        ]        
        
        results = list(db.july2021_all.aggregate(query))
        
        posts_dic[p] = results
            
    return posts_dic


def remove_bots(patterns, posts_dic, bots_names, author_attribute='author'):
    '''
    A function to remove those authors that we know for sure are bots.
    patterns: a list with the names of the different patterns (this names are the same of the dictionaries keys)
    posts_dic: dictionary that will contain the objects queried (keys are the patterns)
    bots_names: a list with the names (string) of the bots
    author_attribute: mongoDB attribute that contain the bot name
    '''
    for p in patterns:
        for name in bots_names:
            posts_dic[p] = [author for author in posts_dic[p] if author[author_attribute] != name]

    return posts_dic

def assign_GenderAge(patterns, posts_dic, regex_dic, attribute_dic, db_year, text_attribute= 'body'):
    posts = []
    signs = [' ', '(', ')', '', '.']

    for p in patterns:
        for post in posts_dic[p]:
            text = post['body']
            match = re.search(regex_dic[p], text)
            post['regex_match'] = match.group(0)
            post['match_index'] = list(match.span())
            match2 = match.group(0)
            for sign in signs:
                match2 = match2.replace(sign, '')

            if match2[attribute_dic[p]['g']].lower() == 'f':
                post['gender'] = 'female'
            elif match2[attribute_dic[p]['g']].lower() == 'm':
                post['gender'] = 'male'
            else: 
                continue
            try:
                post['age'] = match2[attribute_dic[p]['y1']:attribute_dic[p]['y2']]
                post['birth_year'] = db_year - int(post['age'])
                posts.append(post)
            except:
                continue
    return posts

def group_by_author(posts_list, database_month):
    authors_dic =  {}
    for post in posts_list:
        post_info = {"post_id": post['id'],
                     "regex_match": post['regex_match'],
                     "match_index": post['match_index'],
                     "subreddit_with_prefix": post['subreddit_name_prefixed'],
                     "database_month": database_month}
        if post['_id'] not in authors_dic:
            authors_dic[post['author_fullname']] = {"author_id": post['author_fullname'],
                                        "labels": {'birth_year': {},
                                                  'gender': {}}}
            authors_dic[post['author_fullname']]['labels']['birth_year'][post['birth_year']] = [post_info]
            authors_dic[post['author_fullname']]['labels']['gender'][post['gender']] = [post_info]
        else:
            if post['birth_year'] in authors_dic[post['author_fullname']]['labels']['birth_year'].keys():
                authors_dic[post['author_fullname']]['labels']['birth_year'][post['birth_year']].append(post_info)
            if post['gender'] in authors_dic[post['author_fullname']]['labels']['gender'].keys():
                authors_dic[post['author_fullname']]['labels']['gender'][post['gender']].append(post_info)
            if post['birth_year'] not in authors_dic[post['_id']]['labels']['birth_year'].keys():
                authors_dic[post['author_fullname']]['labels']['birth_year'][post['birth_year']] = [post_info]
            if post['age'] not in authors_dic[post['_id']]['labels']['gender'].keys():
                authors_dic[post['author_fullname']]['labels']['gender'][post['gender']] = [post_info]
    return authors_dic
            

def export_authors(authors_dic):
    authors_list = []
    for author in authors_dic.keys():
        authors_list.append(authors_dic[author])
    
    # insert list of objects into collection
    result = db.labelled_authors_intermediate.insert_many(authors_list)
    # print number of inserted documents and their IDs
    print('Inserted ', len(result.inserted_ids))

        
        
database_month = '07-2021'
db_year = 2021
path_intermediate = 'age_gender/'
bots_names = ['[deleted]', 'AutoModerator', 'R_Amods', 'Judgement_Bot_AITA', 'transcribot']

patterns = ['(YYG)', '(GYY)', 'YYG', 'GYY']


regex_dic = {'(YYG)': "(^|\s)(My|my|I|I am|I'm)\s\([0-9][0-9][MmfF]\)(.|,|\s|$)",
             '(GYY)': "(^|\s)(My|my|I|I am|I'm)\s\([MmfF][0-9][0-9]\)(.|,|\s|$)",
             'YYG': "(^|\s)(My|my|I|I am|I'm)(,|, )[0-9][0-9][MmfF](.|,|\s|$)",
             'GYY': "(^|\s)(My|my|I|I am|I'm)(,|, )[MmfF][0-9][0-9](.|,|\s|$)"
            }

posts_dic = {'(YYG)': None, '(GYY)': None, 'YYG':None, 'GYY': None}

attribute_dic = {'(YYG)': {'g': -1, 'y1': -3, 'y2':-1},
                 '(GYY)': {'g': -3, 'y1': -2, 'y2':None},
                 'YYG': {'g': -1, 'y1':-3, 'y2': -1},
                 'GYY': {'g': -3, 'y1':-2, 'y2': None}
                }

posts_dic = query_mongoDB(patterns, regex_dic, posts_dic)

posts_dic = remove_bots(patterns, posts_dic, bots_names)

posts = assign_GenderAge(patterns, posts_dic, regex_dic, attribute_dic, db_year)

authors_dic = group_by_author(posts, database_month)

export_authors(authors_dic)
