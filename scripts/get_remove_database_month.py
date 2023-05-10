import logging
from pymongo import MongoClient
import os
import subprocess
from json import loads
from typing import Union


def decompress_file(filepath: str) -> None:
    result = subprocess.run(['unzstd', '--long=31', filepath])
    if result.returncode != 0:
        raise Exception("Decompression failed")


def decompress_submissions(month_name: str) -> None:
    filepath = '/data/airflow/reddit/submissions/RS_' + month_name + '.zst'

    logging.info(
        'Starting decompression of submissions for month' + month_name)
    decompress_file(filepath)
    logging.info("Decompression completed")


def decompress_comments(month_name: str) -> None:
    filepath = '/data/airflow/reddit/comments/RC_' + month_name + '.zst'

    logging.info('Starting decompression of comments for month' + month_name)
    decompress_file(filepath)
    logging.info("Decompression completed")


def load_documents_to_mongodb(filepath: str, collection) -> None:
    result = subprocess.run(['mongoimport', '--uri', 'mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research',
                                '--collection', collection.name, '--file', filepath]) 
    if result.returncode != 0:
        raise Exception("mongoimport failed")
    
    logging.info("mongoimport completed")


def get_remove_database_month(get_database_month: Union[str, None], remove_database_month: Union[str, None], **kwargs) -> None:
    client = MongoClient(
        'mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')

    if get_database_month or remove_database_month:

        if remove_database_month:

            if remove_database_month not in client.research.list_collection_names():
                raise Exception(
                    f"Collection {remove_database_month} does not exist")

            existing_collection = client.research[remove_database_month]

            existing_collection.drop()

            logging.info(
                f'Database month {remove_database_month} removed from MongoDB')

        if get_database_month:

            collection = client.research[get_database_month]

            if not kwargs.get('no_comments'): # comments files are the large ones
                decompress_comments(get_database_month)

                decompressed_comments_filepath: str = '/data/airflow/reddit/comments/RC_' + \
                    get_database_month

                load_documents_to_mongodb(
                    decompressed_comments_filepath, collection)

                os.remove(decompressed_comments_filepath)

                logging.info(
                    'Comments added to MongoDB. Decompressed comments file removed')

            if not kwargs.get('no_submissions'):
                decompress_submissions(get_database_month)

                decompressed_submissions_filepath: str = '/data/airflow/reddit/submissions/RS_' + \
                    get_database_month

                load_documents_to_mongodb(
                    decompressed_submissions_filepath, collection)

                os.remove(decompressed_submissions_filepath)

                logging.info(
                    'Submissions added to MongoDB. Decompressed submissions file removed')

            logging.info(
                f'Finished adding database month {get_database_month} to MongoDB')

    else:
        logging.info("No database month loaded or removed")


if __name__ == "__main__":
    get_remove_database_month(remove_database_month=None, get_database_month='2022-12', no_submissions=True)