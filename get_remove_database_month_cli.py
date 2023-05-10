import urllib3
from pymongo import MongoClient
import os
import time
import subprocess
from json import loads
import argparse

CHUNK_SIZE = 8192

parser = argparse.ArgumentParser(description='Download a Reddit database from pushshift.io and add it to a MongoDB collection')
parser.add_argument('--remove-month', type=str, help='The year and month of the database month to remove the format YYYY-MM')
parser.add_argument('--get-month', type=str, help='The year and month of the database to download in the format YYYY-MM')
args = parser.parse_args()

get_month_name: str or None = args.get_month
remove_month_name: str or None = args.remove_month

get_month_name = "2021-07"
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

    filepath = get_month_name + ".zst"

    if url is not None:
        # Download the file
        http = urllib3.PoolManager()

        try:
            response = http.request('GET', url, preload_content=False)
            if response.status != 200:
                raise ValueError('Invalid response')

            with open(filepath, 'wb') as f:
                start_time = time.time()
                for i, chunk in enumerate(response.stream(CHUNK_SIZE)):
                    if chunk:
                        f.write(chunk)
                    else:
                        break

                    if i != 0 and i % 1000 == 0:
                        elased_time = time.time() - start_time
                        downloaded_size = (
                            i * CHUNK_SIZE) / (1024 ** 2)
                        total_size = int(
                            response.headers['Content-Length']) / (1024 ** 2)
                        speed = downloaded_size / elased_time
                        remaining_time = (
                            total_size - downloaded_size) / speed

                        print(f"""
                        Downloaded {downloaded_size:.2f} MB of {total_size:.2f} MB 
                        Speed: {speed:.2f} MB/s
                        Time remaining: {remaining_time:.2f} s
                        """)

            if os.path.getsize(filepath) != int(response.headers['Content-Length']):
                raise ValueError('Downloaded file is incomplete')

        except (urllib3.exceptions.HTTPError, ValueError) as e:
            raise Exception(f'HTTP request failed: {e}')

        finally:
            response.release_conn()
    else:
        raise Exception("The url for the database month could not be found. Please check if the database month is available on pushshift.io")

    # Decompress the file
    subprocess.run(['unzstd', '--long=31', filepath])
    decompressed_filename: str = filepath.split('.')[0]

    # Add data to MongoDB
    collection = client.research[get_month_name]

    with open(decompressed_filename, "r") as f:
        for line in f:
            collection.insert_one(dict(sorted(loads(line).items())))

    # Remove the compressed and decompressed files
    os.remove(decompressed_filename)
    os.remove(filepath)

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

