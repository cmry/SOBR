import urllib.request
from pymongo import MongoClient
import os
import subprocess
from json import loads


def get_remove_database_month(get_database_month: str or None, remove_database_month: str or None):
    client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             

    if get_database_month or remove_database_month:

        if remove_database_month:

            if remove_database_month not in client.research.list_collection_names():
                raise Exception(f"Collection {remove_database_month} does not exist")

            existing_collection = client.research[remove_database_month]

            existing_collection.drop()

        if get_database_month:

            url = None
            # Get the url from the db_month_urls.txt file
            with open('/data/airflow/dags/scripts/db_month_urls.txt', 'r') as f:
                for line in f:
                    if get_database_month in line:
                        url = line.strip('\n')

            filename = get_database_month + ".zst"

            if url is not None:
                urllib.request.urlretrieve(url, filename)
            else:
                raise Exception("The url for the database month could not be found. Please check if the database month is available on pushshift.io")

            # Decompress the file
            subprocess.run(['unzstd', '--long=31', filename])
            decompressed_filename: str = filename.split('.')[0]

            # Add data to MongoDB
            collection = client.research[get_database_month]

            with open(decompressed_filename, "r") as f:
                for line in f:
                    collection.insert_one(dict(sorted(loads(line).items())))

            # Remove the compressed and decompressed files
            os.remove(decompressed_filename)
            os.remove(filename)

            print(f'Database month {get_database_month} added to MongoDB')


    else:
        print("No database month downloaded or removed")        