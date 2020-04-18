from db_ops import copy_docs, Client, dbncol
from config import host, port
# setup collection to be copied from
client = Client(host, port)
database = 'OWM_stable'
collection = 'observed'
col = dbncol(client, collection, database=database)
copy_docs(col, 'OWM', 'observed_archive', delete=True)
# def copy_docs(col, destination_db, destination_col, filters={}, delete=False):
#     ''' move or copy a collection within and between databases 
    
#     :param col: the collection to be copied
#     :type col: a pymongo collection
#     :param destination_col: the collection you want the documents copied into
#     :type destination_col: a pymongo.collection.Collection object
#     :param destination_db: the database with the collection you want the documents copied into
#     :type destination_db: a pymongo database pymongo.databse.Database
#     :param filters: a filter for the documents to be copied from the collection. By default all collection docs will be copied
#     :type filters: dict
#     '''
    
#     original = col.find(filters).batch_size(1000)
#     copy = []
#     for item in original[:10]:
#         copy.append(item)
#     destination = dbncol(client, collection=destination_col, database=destination_db)
#     inserted_ids = destination.insert_many(copy).inserted_ids # list of the doc ids that were successfully inserted
#     if delete == True:
#         # remove all the documents from the origin collection
#         for item in inserted_ids:
#             filter = {'_id': item}
#             col.delete_one(filter)
#         print(f'MOVED docs from {col} to {destination}, that is {destination_db}.{destination_col}')
#     else:
#         print(f'COPIED docs in {col} to {destination}, that is {destination_db}.{destination_col}')
