from bs4 import BeautifulSoup
import requests
from typing import List, Tuple

response = requests.get('https://files.pushshift.io/reddit/comments/')

main_url = 'https://files.pushshift.io/reddit/comments/'

soup: BeautifulSoup = BeautifulSoup(response.text, 'html.parser')

paths: List[str] = []

for link in soup.find_all('a'):

    if link.get('href') == '../':
        continue
    else:
        paths.append(link.get('href'))
    
paths = [path for path in paths if path.endswith('.zst')]
paths = list(set(paths))
paths.sort()
paths = [main_url + path.strip('./') for path in paths]

with open('db_month_urls.txt', 'w') as f:
    for path in paths:
        f.write(path + '\n') 