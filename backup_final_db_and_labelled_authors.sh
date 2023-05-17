
#create a variable to store the current month that is passed to the script
query_month=$1

mongodump --uri='mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research' --collection=final_db --gzip --archive=/data/GRASP/backups/final_db/final_db_$query_month.gz
mongodump --uri='mongodb://sergey:topsecretpasswordforsergeysmongo@localhost:27010/research' --collection=labelled_authors --gzip --archive=/data/GRASP/backups/labelled_authors/labelled_authors_$query_month.gz