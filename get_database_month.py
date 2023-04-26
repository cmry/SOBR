import urllib.request
from pymongo import MongoClient
import os
import subprocess
from json import loads
from typing import List


# Download the compressed file from the URL
url = "https://files.pushshift.io/reddit/comments/RC_2005-12.zst"
database_month_name: str = url.split('_')[-1].split('.')[0]
filename: str = database_month_name + ".zst"


urllib.request.urlretrieve(url, filename)

# Decompress the file
subprocess.run(['unzstd', '--long=31', filename])
decompressed_filename: str = filename.split('.')[0]


# Connect to MongoDB and add the file to a collection
client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
db = client.research
collection = db[database_month_name]

with open(decompressed_filename, "r") as f:
    for line in f:
        collection.insert_one(dict(sorted(loads(line).items())))

# Remove the decompressed file
os.remove(decompressed_filename)