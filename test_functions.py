import pytest


def merge_documents(old_document, new_document):

    for key, value in new_document.items():
        
        if key in old_document and isinstance(old_document[key], dict) and isinstance(value, dict):
            merge_documents(old_document[key], value)
            
        elif key in old_document and isinstance(old_document[key], list) and isinstance(value, list):
            old_document[key] += value
            
        else:
            old_document[key] = value

    return old_document


def test_merge_documents_new_attribute():
    """
    GIVEN: Two documents with the same author_id and each has a different attribute
    WHEN: The documents are merged
    THEN: The old document should have the new attribute and the corresponding posts
    """
    old_document = { 'author_id': '1234',
                    'labels': {
                        'nationality': {
                            'Denmark': [
                                {'post_id': '1234', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'},
                                {'post_id': '1235', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'}
                            ]
                        }   
                    }
    }

    new_document = { 'author_id': '1234',
                    'labels': {
                        'personality': {
                            'introvert': [
                                {'post_id': '1236', 'flair': 'introvert', 'subreddit_with_prefix': 'r/introvert', 'database_month': '07-2021'},
                            ]
                        }
                    }
    }
    
    merge_documents(old_document, new_document)
     
    results_document = { 'author_id': '1234',
                            'labels': {
                                    'nationality': {
                                        'Denmark': [
                                            {'post_id': '1234', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'},
                                            {'post_id': '1235', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'}
                                        ]
                                    }
                                    ,
                                    'personality': {
                                        'introvert': [
                                            {'post_id': '1236', 'flair': 'introvert', 'subreddit_with_prefix': 'r/introvert', 'database_month': '07-2021'},
                                        ]
                                    }
                            }
    }    
    assert old_document == results_document
    

def test_merge_documents_new_label_same_attribute():
    """
    GIVEN: Two documents with the same author_id and each has a different label for the same attribute
    WHEN: The documents are merged
    THEN: The old document should have the new label and the corresponding posts
    """
    old_document = { 'author_id': '1234',
                    'labels': {
                        'nationality': {
                            'Denmark': [
                                {'post_id': '1234', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'},
                                {'post_id': '1235', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'}
                            ]
                        }   
                    }
    }

    new_document = { 'author_id': '1234',
                    'labels': {
                        'nationality': {
                            'Sweden': [
                                 {'post_id': '1236', 'flair': 'Sweden', 'subreddit_with_prefix': 'r/sweden', 'database_month': '07-2021'},
                            ]
                        }
                    }
    }
    
    merge_documents(old_document, new_document)
    
    results_document = { 'author_id': '1234',
                            'labels': {
                                'nationality': {
                                    'Denmark': [
                                        {'post_id': '1234', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'},
                                        {'post_id': '1235', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'}
                                    ],
                                    'Sweden': [
                                        {'post_id': '1236', 'flair': 'Sweden', 'subreddit_with_prefix': 'r/sweden', 'database_month': '07-2021'},
                                    ]
                                }
                            }
    }
    
    assert old_document == results_document
    

def test_merge_documents_new_posts_existing_label():
    """
    GIVEN: Two documents with the same author_id and the same label for the same attribute
    WHEN: The documents are merged
    THEN: The old document should have the new posts
    """                                   

    old_document = { 'author_id': '1234',
                    'labels': {
                        'nationality': {
                            'Denmark': [
                                {'post_id': '1234', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'},
                                {'post_id': '1235', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'}
                            ]
                        }   
                    }
    }
    
    new_document = { 'author_id': '1234',
                    'labels': {
                        'nationality': {
                            'Denmark': [
                                {'post_id': '5678', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '06-2021'},
                                {'post_id': '5679', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '06-2021'}
                            ]
                        }   
                    }
    }
    
    merge_documents(old_document, new_document)
    
    
    results_documents = { 'author_id': '1234',
                    'labels': {
                        'nationality': {
                            'Denmark': [
                                {'post_id': '1234', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'},
                                {'post_id': '1235', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '07-2021'},
                                {'post_id': '5678', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '06-2021'},
                                {'post_id': '5679', 'flair': 'Denmark', 'subreddit_with_prefix': 'r/europe', 'database_month': '06-2021'}
                            ]
                        }   
                    }
    }
    
    assert old_document == results_documents
    
test_merge_documents_new_attribute()
test_merge_documents_new_label_same_attribute()
test_merge_documents_new_posts_existing_label()