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
    
    def count(self):
        ''' Get the count of the elements in self.casts. '''
        
        return len(self.casts)
    
    def itslegit(self):
        ''' Check the instant's weathers array's count and if it is 40, then the
        document is returned.

        :param instant: the instant docuemnt to be legitimized
        :type instant: dictionary
        '''
        
        if len(self.casts) >= 40:
            return True
        else:
            return False
    
    def to_dbncol(self, collection='test'):
        ''' Load the data to the database. 

        :param collection: the collection name
        :type collection: string
        '''

        from db_ops import dbncol

        col = dbncol(client, collection, database)  # Remember client and
                                                    # database are in global
        col.update_one({'_id': self._id}, {'$set': self.as_dict}, upsert=True)


def cast_count_all(instants):
    ''' get a tally for the forecast counts per document 

    :param instants: Instant class objects
    :type instants: list
    '''

    n = 0
    collection_cast_counts = {}

    # Go through each doc in the collection and count the number of items in
    # the forecasts array, then add to the tally for that count.
    for doc in instants:
        n = count(doc)
        if n in collection_cast_counts:
            collection_cast_counts[n] += 1
        else:
            collection_cast_counts[n] = 1
    return collection_cast_counts


def sweep(instants):
    ''' Move any instant that has a ref_time less than the current next
    ref_time and with self.count less than 40. This is getting rid of the
    instnats that are not and will never be legit.

    :param instants: a list of instnant objects
    '''

    
def find_legit(instants):
    ''' Find the 'legit' instants within the list.

    :param instants: all the instants pulled from the database
    :type instants: list
    :return: list of instants with a complete forecasts array
    '''

    return [item for item in instants if item.itslegit()]


def load_legit(legit_list):
    ''' Load the 'legit' instants to the remote database and delete from temp.

    :param collection: the collection you want to pull instants from
    :type collection: pymongo.collection.Collection
    '''

    from db_ops import dbncol
    col = dbncol(client, 'legit_inst', 'owmap')
    col.insert_many(legit_list)
    # Now go to the temp_instants collection and delete the instants just
    # loaded to legit_inst.
    col = dbncol(client, 'temp_inst', 'owmap')
    col.delete_many(legit_list)
    return        

def test_load_legit(legit_list):
    ''' Load the 'legit' instants to the remote database and delete from temp.

    :param collection: the collection you want to pull instants from
    :type collection: pymongo.collection.Collection
    '''

    from db_ops import dbncol
    col = dbncol(client, 'legit_inst', database)
    col.insert_many(legit_list)
    # Now go to the temp_instants collection and delete the instants just
    # loaded to legit_inst.
    col = dbncol(client, 'temp_inst', database)
    col.delete_many(legit_list)
    return        
