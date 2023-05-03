import urllib.request
from pymongo import MongoClient
import os
import subprocess
from json import loads
import argparse

parser = argparse.ArgumentParser(description='Download a Reddit database from pushshift.io and add it to a MongoDB collection')
parser.add_argument('--remove-month', type=str, help='The year and month of the database month to remove the format YYYY-MM')
parser.add_argument('--get-month', type=str, help='The year and month of the database to download in the format YYYY-MM')
args = parser.parse_args()

get_month_name: str or None = args.get_month
remove_month_name: str or None = args.remove_month

if not get_month_name and not remove_month_name:
    raise Exception("Please provide either the --get-month or --remove-month argument")

client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             

if get_month_name:

    url = None
    # Get the url from the db_month_urls.txt file
    with open('db_month_urls.txt', 'r') as f:
        for line in f:
            if get_month_name in line:
                url = line.strip('\n')

    filename = get_month_name + ".zst"

    if url is not None:
        urllib.request.urlretrieve(url, filename)
    else:
        raise Exception("The url for the database month could not be found. Please check if the database month is available on pushshift.io")

    # Decompress the file
    subprocess.run(['unzstd', '--long=31', filename])
    decompressed_filename: str = filename.split('.')[0]

    # Add data to MongoDB
    collection = client.research[get_month_name]

    with open(decompressed_filename, "r") as f:
        for line in f:
            collection.insert_one(dict(sorted(loads(line).items())))

    # Remove the compressed and decompressed files
    os.remove(decompressed_filename)
    os.remove(filename)

    print(f'Database month {get_month_name} added to MongoDB')

if remove_month_name:

    if remove_month_name not in client.research.list_collection_names():
        raise Exception(f"Collection {remove_month_name} does not exist")

    existing_collection = client.research[remove_month_name]

    choice = input(f"Are you sure you want to remove collection {remove_month_name}? (y/n) ")

    if choice == 'y':
        existing_collection.drop()
        print(f"Collection {remove_month_name} removed")
    else:
        print(f"Collection {remove_month_name} not removed")
