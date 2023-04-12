from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import statistics as stats
from bson.json_util import dumps, loads
from time import time

client = MongoClient("localhost", 27010)
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")
db.collection_names()

t = time()
final_db = list(db.final_db.find())
df = pd.DataFrame(final_db)
df = df.where(pd.notnull(df), None)
print('It took', round((time()-t)/60, 2), 'minutes')

print('df shape:', str(df.shape))
#print(str(df.columns))
df.head()

binary_vars = {'gender':{'male': 'male', 'female': 'female'}, 
               'personality_D1':{'extroversion': 'personality_extrovert', 'introversion': 'personality_introvert'},
               'personality_D2': {'sensors': 'personality_sensing', 'intuitives': 'personality_intuitive'},
               'personality_D3':{'thinkers': 'personality_thinking', 'feelers': 'personality_feeling'},
               'personality_D4':{'judgers': 'personality_judging', 'perceivers': 'personality_perceiving'}
              }

n_posts_per_label = {'gender':{'male': None, 'female': None},
                     'personality_D1':{'extroversion': None, 'introversion': None},
                     'personality_D2':{'sensors': None, 'intuitives': None},
                     'personality_D3':{'thinkers': None, 'feelers': None},
                     'personality_D4':{'judgers': None, 'perceivers': None},
                     'political_leaning': {},
                     'nationality': {},
                     'birth_year': {}
                    }

percentage_posts_per_label = {'gender':[{'male': None, 'female': None}, None, None],
                              'personality_D1':[{'extroversion': None, 'introversion': None}, None, None],
                              'personality_D2':[{'sensors': None, 'intuitives': None}, None, None],
                              'personality_D3':[{'thinkers': None, 'feelers': None}, None, None],
                              'personality_D4':[{'judgers': None, 'perceivers': None}, None, None],
                              'political_leaning': [{}, None, None],
                              'nationality': [{}, None, None],
                              'birth_year': [{}, None, None]
                             }

# count number of posts per label (binary variables)
for attribute in binary_vars.keys():
  for label in binary_vars[attribute].keys():
    n_posts_per_label[attribute][label] = df[binary_vars[attribute][label]].sum()
        
# count number of posts per label (categorical variables)
categorical_variables = ['nationality', 'political_leaning', 'birth_year']

for var in categorical_variables:
  labels = df[var].unique()
  labels = [x for x in labels if x is not None]
  if var == 'birth_year': 
    # this orders birth year chronologically
    labels = sorted([int(x) for x in labels])
    for label in labels:
      n_posts_per_label[var][str(label)] = df[var].value_counts()[str(label)]

# from most frequent to less frequent nationality
n_posts_per_label['nationality'] = dict(sorted(n_posts_per_label['nationality'].items(),
                                               key=lambda x:x[1], reverse = True))

for attribute in n_posts_per_label.keys():
  tot = 0
  for label in n_posts_per_label[attribute]:
    tot+= n_posts_per_label[attribute][label]
    for label in n_posts_per_label[attribute]:
      percentage_posts_per_label[attribute][0][label] = round((n_posts_per_label[attribute][label]/tot)*100, 2)
      percentage_posts_per_label[attribute][1] = round(tot)
      percentage_posts_per_label[attribute][2] = round((tot/len(df))*100, 2)

# sort birth years by most frequent
percentage_posts_per_label['birth_year'][0] = dict(sorted(percentage_posts_per_label['birth_year'][0].items(),
                                                          key=lambda x:x[1], reverse = True))

# each attribute lists contain percentage of specific labels in the attribute's total posts, 
#number of posts of that attribute, percentage occupied by that attribute in the df (NB. personality is 5% in total)
percentage_posts_per_label

# Plot nationalities
labels = []
sizes = []

for x, y in n_posts_per_label['nationality'].items():
  labels.append(x)
  sizes.append(y)

# Plot
plt.pie(sizes, labels=labels, radius = 10)

plt.axis('equal')
plt.show()

# look into birth year 
birth_year = [int(x) for x in list(df['birth_year']) if x is not None]
pd.DataFrame({'col':birth_year})['col'].describe()

# Plot birthyear
plt.bar(n_posts_per_label['birth_year'].keys(), n_posts_per_label['birth_year'].values())


### Posts length
# full df
posts_length = []
for post in df['post_body']:
  n = len(post.split())
  posts_length.append(n)
    
pd.DataFrame({'l':posts_length})['l'].describe()

# by attribute
postsLength_per_label = {'gender':{'male': None, 'female': None},
                         'personality_D1':{'extroversion': None, 'introversion': None},
                         'personality_D2':{'sensors': None, 'intuitives': None},
                         'personality_D3':{'thinkers': None, 'feelers': None},
                         'personality_D4':{'judgers': None, 'perceivers': None},
                         'political_leaning': {},
                         'nationality': {},
                         'birth_year': {}
                        }

# count number of posts per label (binary variables)
for attribute in binary_vars.keys():
  for label in binary_vars[attribute].keys():
    posts = df.loc[df[binary_vars[attribute][label]] == 1, 'post_body']
    posts_length = []
    for post in posts:
      n = len(post.split())
      posts_length.append(n)
    measures = {}
    measures['mean'] = round(stats.mean(posts_length), 2)
    measures['median'] = round(stats.median(posts_length), 2)
    measures['st.dv'] = round(stats.pstdev(posts_length), 2)
    measures['min'] = min(posts_length)
    measures['max'] = max(posts_length)
    postsLength_per_label[attribute][label] = measures
    
# count number of posts per label (categorical variables)
for attribute in categorical_variables:
  labels = df[attribute].unique()
  labels = [x for x in labels if x is not None]
  if attribute == 'birth_year':
    labels = sorted([int(x) for x in labels])
  for label in labels:
    posts = df.loc[df[attribute] == str(label), 'post_body']
    posts_length = []
    for post in posts:
      n = len(post.split())
      posts_length.append(n)
    measures = {}
    measures['mean'] = round(stats.mean(posts_length), 2)
    measures['median'] = round(stats.median(posts_length), 2)
    measures['st.dv'] = round(stats.pstdev(posts_length), 2)
    measures['min'] = min(posts_length)
    measures['max'] = max(posts_length)
    
    postsLength_per_label[attribute][label] = measures




