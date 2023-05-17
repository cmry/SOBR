from pymongo import MongoClient
import logging


def create_main_collection_indices(database_month: str, **kwargs) -> None:
    client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
    collection = client.research[database_month]

    collection.create_index('author_fullname')
    logging.info('Created index for author_fullname')

    collection.create_index('subreddit_name_prefixed')
    logging.info('Created index for subreddit_name_prefixed')

    collection.create_index('author_flair_text')
    logging.info('Created index for author_flair_text')
