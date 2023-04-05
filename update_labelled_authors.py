from pymongo import MongoClient

#client = MongoClient("localhost", 27010)             
client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
db = client.research
#db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

labelled_authors_ids = set(db.labelled_authors.distinct('author_id'))
labelled_authors_intermediate_ids = set(db.labelled_authors_intermediate.distinct('author_id'))

authors_to_update = labelled_authors_ids.intersection(labelled_authors_intermediate_ids)
new_authors = labelled_authors_intermediate_ids.difference(authors_to_update)

for author_id in authors_to_update:
    
    old_data = list(db.labelled_authors.find({'author_id': author_id}))
    assert len(old_data) == 1, f"There should be only one author with id {author_id}"
    
    old_data = set(old_data[0])
    
    new_data = list(db.labelled_authors_intermediate.find({'author_id': author_id}))
    assert len(new_data) == 1, f"There should be only one author with id {author_id}"
    
    new_data = set(new_data[0])
    
    