from pymongo import MongoClient
from tqdm import tqdm


def update_labelled_authors():
    client = MongoClient('mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research?authSource=research')             
    db = client.research


    def merge_documents(old_document, new_document):

        for key, value in new_document.items():
            
            if key in old_document and isinstance(old_document[key], dict) and isinstance(value, dict):
                merge_documents(old_document[key], value)
                
            elif key in old_document and isinstance(old_document[key], list) and isinstance(value, list):
                old_document[key] += value
                
            else:
                old_document[key] = value

        return old_document


    labelled_authors_ids = set(db.labelled_authors.distinct('author_id'))
    labelled_authors_intermediate_ids = set(db.labelled_authors_intermediate.distinct('author_id'))

    authors_to_update = labelled_authors_ids.intersection(labelled_authors_intermediate_ids)
    new_authors = labelled_authors_intermediate_ids.difference(authors_to_update)


    # Update authors in the labelled_authors collection with new labels and posts
    updated_documents = []

    for author_id in tqdm(authors_to_update, total=len(authors_to_update), desc="Updating authors"):
        
        old_data = list(db.labelled_authors.find({'author_id': author_id}))
        assert len(old_data) == 1, f"There should be only one author with id {author_id}"
        
        old_document = old_data[0]
        
        new_data = list(db.labelled_authors_intermediate.find({'author_id': author_id}))
        assert len(new_data) == 1, f"There should be only one author with id {author_id}"
        
        new_document = new_data[0]

        merge_documents(old_document, new_document)
        
        updated_documents.append(old_document)

    # Replace the old documents with the updated ones
    try:
        deletion_result = db.labelled_authors.delete_many({'author_id': {'$in': list(authors_to_update)}})
        assert deletion_result.deleted_count == len(authors_to_update), f"Deleted {deletion_result.deleted_count} documents, but there were {len(authors_to_update)} documents to delete"
    except TypeError:
        pass # There are no authors to update

    try:
        update_result = db.labelled_authors.insert_many(updated_documents)
        assert len(update_result.inserted_ids) == len(authors_to_update), f"Inserted {len(update_result.inserted_ids)} documents, but there were {len(authors_to_update)} documents to insert"
    except TypeError:
        pass # There are no authors to update

    # Insert the new documents
    insertion_result = None
    try:
        new_documents = list(db.labelled_authors_intermediate.find({'author_id': {'$in': list(new_authors)}}))
        insertion_result = db.labelled_authors.insert_many(new_documents)
        assert len(insertion_result.inserted_ids) == len(new_authors), f"Inserted {len(insertion_result.inserted_ids)} documents, but there were {len(new_authors)} documents to insert"
    except TypeError:
        pass # There are no new authors to insert


    # Empty the intermediate collection
    db.labelled_authors_intermediate.drop()

    return len(insertion_result.inserted_ids) if insertion_result else 0 # Return the number of new authors inserted
 
        