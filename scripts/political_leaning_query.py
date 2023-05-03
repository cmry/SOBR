from pymongo import MongoClient
from tqdm import tqdm

def political_leaning_query(database_month: str):
    client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
    db = client.research

    patterns = ['right', 'left', 'center']
    subreddits = ['r/PoliticalCompassMemes', 'r/PoliticalCompass', 'r/PCM_University']

    posts_dic = {'right': None,
                'left': None,
                'center': None
                }

    regex_dic = {'right': r'(^|\s)(LibRight|Right|AuthRight)(\s|$)',
                'left': r'(^|\s)(LibLeft|Left|AuthLeft)(\s|$)',
                'center':  r'(^|\s)(LibCenter|Centrist|AuthCenter)(\s|$)'
                }

    def query_mogoDB(patterns, regex_dic, posts_dic):
        '''
        A function to query a mongoDB collection selecting only those documents which text attribute contains the text of interest
        patterns: a list with the names of the different patterns (this names are the same of the dictionaries keys)
        regex_dic: a dictionary that link every pattern name to the actual pattern to be searched with regex
        db_file: mongoDB collection
        posts_dic: dictionary that will contain the objects queried (keys are the patterns)
        path: where the function is going to save the queriesd posts
        database_month: label to indicate the mongoBB collection (when saving)
        text_attribute: mongoDB attribute to query'''
        for p in tqdm(patterns, total = len(patterns)):

            query = [
                {'$match': {'subreddit_name_prefixed': {'$in': subreddits}}},
                {'$match': {'author_flair_text': {'$regex': regex_dic[p]}}},
            ]        
            
            results = list(db[database_month].aggregate(query))
            posts_dic[p] = results
            
        return posts_dic

    def assign_PolPosition(patterns, posts_dic):
        posts = []
        for p in patterns:
            for post in posts_dic[p]:
                post['political_leaning'] = p
                posts.append(post)
        return posts

    def group_by_author(posts_list, database_month):
        authors_dic =  {}
        for post in posts_list:
            post_info = {"post_id": post['id'],
                        "flair": post['author_flair_text'],
                        "subreddit_with_prefix": post['subreddit_name_prefixed'],
                        "database_month": database_month}
            if post['_id'] not in authors_dic:
                authors_dic[post['author_fullname']] = {"author_id": post['author_fullname'],
                                                        "labels": {'political_leaning': {}}}
                
                authors_dic[post['author_fullname']]['labels']['political_leaning'][post['political_leaning']] = [post_info]
            else:
                if post['political_leaning'] in authors_dic[post['author_fullname']]['labels']['political_leaning'].keys():
                    authors_dic[post['author_fullname']]['labels']['political_leaning'][post['political_leaning']].append(post_info)
                
                if post['political_leaning'] not in authors_dic[post['author_fullname']]['labels']['political_leaning'].keys():
                    authors_dic[post['author_fullname']]['labels']['political_leaning'][post['political_leaning']] = [post_info]
        return authors_dic
                
    def export_authors(authors_dic):
        authors_list = []
        for author in authors_dic.keys():
            authors_list.append(authors_dic[author])
        # insert list of objects into collection
        result = db.labelled_authors_intermediate.insert_many(authors_list)
        print('Inserted', len(result.inserted_ids))
            

    posts_dic = query_mogoDB(patterns, regex_dic, posts_dic)

    posts = assign_PolPosition(patterns, posts_dic)

    authors_dic = group_by_author(posts, database_month)

    export_authors(authors_dic)

