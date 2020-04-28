''' define the instant class and some useful scripts related to them '''

class instant:

    def __init__(instant=int, zipcode=str, coordinates=dict, forecasts=list, observation=dict):
        {'instant': instant,
        'zipcode': zipcode,
         'coordinates': coordinates,
        'forecasts': forecasts,
        'observation': observation
        }
    
    def count(instant):
        ''' Count the number of elemnets in the forecasts array '''
        
        return len(instant['forecats'])
        

def count(instant):
    ''' Count the number of elemnets in the forecasts array '''

    return len(instant['forecasts'])

def cast_count_all(col):
    ''' get a tally for the forecast counts per document 
    
    :param col: the collection you want to evaluate
    :type col: pymongo.collection.Collection
    '''
    print('getting started with cast_count_all')
    n = 0
    collection_cast_counts = {}
    # Go through each doc in the collection and count the number of items in the forecasts array.
    # Add to the tally for that count.
    for doc in col.find({}):
        n = count(doc)
        if n in collection_cast_counts:
            collection_cast_counts[n] += 1
        else:
            collection_cast_counts[n] = 1
    return collection_cast_counts

def itslegit(instant):
    ''' check the instant's weathers array for count. if it is 40, then the document is returned

    :param instant: the instant docuemnt to be legitimized
    :type instant: dictionary
    '''

    if count(instant) == 40:
        return True
    else:
        return False

def find_legit(collection):
    ''' find the 'legit' instants within the collection specified

    :param collection: database collection
    :type collection: pymongo.collection.Collection
    :return: list of documents
    '''

    return [item for item in collection.find({}) if itslegit(item)]

def load_legit(collection):
    ''' load the 'legit' instants to the remote database 

    :param collection: the collection you want to pull instants from
    :type collection: pymongo.collection.Collection
    '''
    from db_ops import Client, dbncol
    from config import uri, host, port

    legit_list = getlegit(collection)
    client = Client(host, port)
    col = dbncol(client, 'legit_inst', 'owmap')
    col.insert_many(legit_list)
    return

def test_load_legit(collection):
    ''' load the 'legit' instants to the remote database 

    :param collection: the collection you want to pull instants from
    :type collection: pymongo.collection.Collection
    '''
    from db_ops import Client, dbncol
    from config import host, port
    host = 'localhost'
    port = 27017
    legit_list = getlegit(collection)
    client = Client(host, port)
    col = dbncol(client, 'legit_inst', 'test')
    col.insert_many(legit_list)
    return