from pymongo import MongoClient
import datetime
from tqdm import tqdm
import logging
from typing import Union


def labelled_authors_to_final_db(database_month: str):
    client = MongoClient(
        'mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')
    db = client.research

    personality_attributes = ['extrovert', 'introvert', 'sensing',
                              'intuitive', 'thinking', 'feeling', 'judging', 'perceiving']
    personality_subreddits = ['r/entj', 'r/enfp', 'r/enfj', 'r/intp', 'r/esfj', 'r/esfp', 'r/infp',
                              'r/intj', 'r/infj', 'r/isfj', 'r/entp', 'r/estp', 'r/estj', 'r/istj', 'r/isfp', 'r/istp']
    nationality_subreddits = ['r/europe', 'r/AskEurope',
                              'r/EuropeanCulture', 'r/EuropeanFederalists', 'r/Eurosceptics']
    political_leaning_subreddits = [
        'r/PoliticalCompassMemes', 'r/PoliticalCompass', 'r/PCM_University']

    pipeline = [
        {'$lookup': {'from': database_month, 'localField': 'author_id',
                     'foreignField': 'author_fullname', 'as': 'post'}},
        {'$unwind': {'path': '$post', 'preserveNullAndEmptyArrays': True}},
    ]

    curser = db.labelled_authors.aggregate(pipeline, allowDiskUse=True)
    entries = []

    for document in tqdm(curser):
        entry: dict[str, Union[str, int, None]] = {}
        post: Union[dict[str, Union[str, int, None]], None] = document.get('post', None)

        if not post: continue # skip authors without posts in the month

        entry['post_id'] = post.get('id', None)
        entry['author_id'] = document['author_id']
        entry['subreddit'] = post.get('subreddit_name_prefixed', None)

        create_date_epoch = post.get('created_utc', None)
        if create_date_epoch:
            entry['created_on'] = datetime.datetime.fromtimestamp(
                create_date_epoch).strftime('%Y-%m-%d %H:%M:%S') # type: ignore

        body = post.get('body', None)
        if post is None: continue # skip deleted posts
        entry['post'] = body

        labels = document['labels']

        if not labels.get('gender'):
            entry['male'] = None
            entry['female'] = None
            entry['gender_source_post'] = None
        else:
            if len(labels['gender'].keys()) > 1:
                continue  # inconsistent label
            elif labels['gender'].get('male'):
                male, female = (1, 0)
            else:
                male, female = (0, 1)

            entry['male'] = male
            entry['female'] = female

            gender = 'male' if male == 1 else 'female'
            entry['gender_source_post'] = 1 if document['post']['id'] in [
                post['post_id'] for post in labels['gender'][gender]] else 0

        if not labels.get('birth_year'):
            entry['birth_year'] = None
            entry['age_source_post'] = None
        else:
            if len(labels['birth_year'].keys()) > 1:
                continue  # inconsistent label
            else:
                year = list(labels['birth_year'].keys())[0]

            entry['birth_year'] = year

            entry['age_source_post'] = 1 if document['post']['id'] in [
                post['post_id'] for post in labels['birth_year'][year]] else 0

        if not labels.get('nationality'):
            entry['nationality'] = None
            entry['nationality_in_domain'] = None
        else:
            if len(labels['nationality'].keys()) > 1:
                continue  # inconsistent label
            else:
                entry['nationality'] = list(labels['nationality'].keys())[0]

            entry['nationality_in_domain'] = 1 if document['post']['subreddit_name_prefixed'] in nationality_subreddits else 0

        if not labels.get('political_leaning'):
            entry['political_leaning'] = None
            entry['political_leaning_in_domain'] = None
        else:
            if len(labels['political_leaning'].keys()) > 1:
                continue  # inconsistent label
            else:
                entry['political_leaning'] = list(
                    labels['political_leaning'].keys())[0]

            entry['political_leaning_in_domain'] = 1 if document['post']['subreddit_name_prefixed'] in political_leaning_subreddits else 0

        if not labels.get('personality'):
            for attribute in personality_attributes:
                key = 'personality_' + attribute
                entry[key] = None
            entry['personality_in_domain'] = None
        else:
            # inconsistent label checks
            if len(labels['personality']['extrovert']) > 0 and len(labels['personality']['introvert']) > 0:
                continue
            if len(labels['personality']['sensing']) > 0 and len(labels['personality']['intuitive']) > 0:
                continue
            if len(labels['personality']['thinking']) > 0 and len(labels['personality']['feeling']) > 0:
                continue
            if len(labels['personality']['judging']) > 0 and len(labels['personality']['perceiving']) > 0:
                continue

            for attribute in personality_attributes:
                key = 'personality_' + attribute
                entry[key] = 1 if len(
                    labels['personality'][attribute]) > 0 else 0

            entry['personality_in_domain'] = 1 if document['post']['subreddit_name_prefixed'] in personality_subreddits else 0

        entries.append(entry)

    number_of_entries = len(entries)

    db.final_db.insert_many(entries)

    logging.info(f"Inserted {number_of_entries} entries into final_db")

if __name__ == '__main__':
    labelled_authors_to_final_db('2022-11')