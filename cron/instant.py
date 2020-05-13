''' Defines the Instant class and some useful functions. '''


class Instant:

    def __init__(self, _id, forecasts=[], observations={}):
        
        self._id = _id
        self.casts = forecasts
        self.obs = observations
        self.as_dict = {'_id': self._id,
                        'forecasts': self.casts,
                        'observations': self.obs
                        }
    
    @property
    def count(self):
        ''' Get the count of the elements in self.casts. '''
        
        return len(self.casts)
    
    @property
    def itslegit(self):
        ''' Check the instant's weathers array's count and if it is 40, then the
        document is returned.

        :param instant: the instant docuemnt to be legitimized
        :type instant: dictionary
        '''
        
        self.count()
        if self.count == 40:
            return True
        else:
            return False
    
    def to_dbncol(self, collection='test'):
        ''' Load the data to the database. 

        :param collection: the collection name
        :type collection: string
        '''

        from config import database
        from db_ops import dbncol

        col = dbncol(client, collection, database=database)
        col.update_one({'_id': self._id}, {'$set': self.as_dict}, upsert=True)

    
def cast_count_all(instants):
    ''' get a tally for the forecast counts per document 

    :param instants: docmuments loaded from the db.instants collection ### NOT
    Instant class objects
    :type instants: list
    '''
    
    n = 0
    collection_cast_counts = {}

    # Go through each doc in the collection and count the number of items in
    # the forecasts array. Add to the tally for that count.
    for doc in instants:
        n = len(doc['forecasts'])
        # Move the legit instants to the permenant database
        if n >= 40:
            load_legit(doc)
        if n in collection_cast_counts:
            collection_cast_counts[n] += 1
        else:
            collection_cast_counts[n] = 1
    return collection_cast_counts


def sweep(instants):
    ''' Move any instant that has a ref_time less than the current next
    ref_time and with self.count less than 40. This is getting rid of the
    instnats that are not and will never be legit. 

    :param instants: a list of Instant objects
    '''
    
    import time
    
    import pymongo
    from pymongo.cursor import Cursor
    from Extract.make_instants import find_data
    from config import client, database
    from db_ops import dbncol
    
    col = dbncol(client, 'instant_temp', database=database)
    n = 0
    # Check the instant type- it could be a dict if it came from the database,
    # or it could be a list if it's a bunch of instant objects, or a pymongo
    # cursor over the database.
    if type(instants) == dict:
        for key, doc in instants:
            if key['instant'] < time.time()-453000:  # 453000sec: about 5 days
                col.delete_one(doc)
                n += 1
    elif type(instants) == list:
        for doc in instants:
            if doc['instant'] < time.time()-453000:
                col.delete_one(doc)
                n += 1
    elif type(instants) == pymongo.cursor.Cursor:
        for doc in instants:
            if doc['instant'] < time.time()-453000:
                col.delete_one(doc)
                n += 1
    else:
        print(f'You want me to sweep instants that are {type(instants)}\'s.')
    return

def find_legit(instants):
    ### THIS DOES NOT WORK ###
    ''' find the 'legit' instants within the list

     :param instants: all the instants pulled from the database
     :type instants: list
     :return: list of instants with a complete forecasts array
     '''

    i = [item for item in instants if len(item['forecasts']) >= 40]
    return f'legit list is {len(i)} items long'
    ### maybe you should make the instant documents pulled form the database
    ### represented as Instants in memory. Then you could use the Instant
    ### methods you've been writing.


def load_legit(legit_list):
    ''' Load the 'legit' instants to the remote database and delete from temp.
    This process does not delete the 

    :param collection: the collection you want to pull instants from
    :type collection: pymongo.collection.Collection
    '''

    from pymongo.errors import DuplicateKeyError
    import config
    from config import remote_client
    from config import client
    from config import database
    from db_ops import dbncol
    # from db_ops import copy_docs
    
### this should load to the remote_client.owmap.legit_inst for production ###
    remote_col = dbncol(remote_client, 'legit_inst', database=database)
    col = dbncol(client, 'instant_temp', database=database)
    # col = dbncol(client, 'legit_inst', database=database)
    try:
        check = remote_col.insert_one(legit_list)
        if not check:
            print('was not able to insert to remote db')
    except DuplicateKeyError:
        col.delete_one(legit_list)
        ### saved for later, when doing it on bulk ###
#     col.insert_many(legit_list)
    # Now go to the temp_instants collection and delete the instants just
    # loaded to legit_inst.
#     col.delete_many(legit_list)
    return


import time
start_time = time.time() # This is to get the total runtime if this script is
                         # run as __main__
if __name__ == '__main__':
    ''' Connect to the database, then move all the legit instants to the remote
    database and clear out any instants that are past and not legit.
    '''
    
    import config
    import db_ops

    # Set the database here if you must, but it's better to do that in the
    # config.py file
#     database = 'owmap'
    collection = 'instant_temp'
    col = db_ops.dbncol(config.client, collection, database=config.database)
    cast_count_all(col.find({}))
    sweep(col.find({}))

    print(f'Total op time for instant.py was {time.time()-start_time} seconds')