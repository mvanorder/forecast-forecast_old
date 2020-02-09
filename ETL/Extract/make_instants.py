from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure
from urllib.parse import quote
from config import OWM_API_key as key, connection_port, user, password, socket_path

port = 27017
host = 'localhost'
# owm = OWM(API_key)    # the OWM object
password = quote(password)    # url encode the password for the mongodb uri
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
print(uri)

# client = MongoClient(uri)
client = MongoClient(host=host, port=port)


def load(data, client, name):
    # because this function is used in a loop, I want to name the variable, {name}, to be whatever the diciontary's
    # name happens to be. That is getting set to a colleciton name 
    ''' Load the data to the database if possible, otherwise write to json file. 
        
        :param data: the dictionary created from the api calls
        :type data: dict
        :param client: the pymongo client object
        :type client: MongoClient
        :param name: the database clooection to be used
    '''
    if type(data) == dict:
        database = client.OWM
        # name = name
        try:
            col = Collection(database, name)
            # db = client.forcast
            # col = db.code
            col.insert_one(data)
        except DuplicateKeyError:
            client.close()
            print('closed db connection')
            to_json(data, code)
            print('Wrote to json')
    else:
        print('data type is not dict')
        client.close()
        print('closed db connection')
    return


try:
    db = client.OWM
        except ConnectionFailure:
    print('ConnectionFailure')
    
col = db.weather
filters = {'zipcode':'27006', }
weathers = col.find(filters)
col.count_documents(filters)
print(f'found {col.count_documents(filters)} documents.')
# print(weather)

col = db.forecast
filters = {'zipcode':'27006'}
forecasts = col.find(filters)
num_of = col.count_documents(filters)
print(f'found {num_of} documents.')
# forecasts.five_day

col = db.weather
filter_by_zip = {'zipcode':'27006', }
filters = filter_by_zip
weathers_by_zip = col.find(filters)
query = weathers_by_zip
instant = {'_id': str,
           'instant': int,
           'location': {},
           'weather': {},
           'forecasts': []
                   }
# Loop through each forecast document with zipcode <zipcode>.  Each of these items has the metadata
# and weather data for the instant to be created.....update the instant dictionary with it.
# 
#I did a lot of switching back and forth trying to figure out wtf was going on with a KeyError (it was at first
#due to waffling on what to call the variables while still creating documents in the database, and then changed and 
#turned out to be that I was indexing the loop variable 'cast' and I shouldn't have been) and wrote different chunks 
#of code to deal with different anomalous data or behavior...those chunks should remain until I know that the inputs
#are being created uniformly
n=0
instants = [] # list to keep track of the 'instants' so far checked--
for item in query:
    instant = {'_id': str,
           'instant': int,
           'location': {},
           'weather': {},
           'forecasts': []
                   }
    if n<=1:
# The try/except's here are catching KeyErrors due to different data uploading scripts
        try:
            if item['instant']  in instants:
                print(f'instant in instants on n={n}')
                n+=1
                continue
        except KeyError:
            if item['current']['Weather']['reference_time'] in instants:
                print(f'Caught KeyError on "instant". ref_time IS in instants on n={n}')
                n+=1
                continue
        try:
            instant.update({'_id': item['zipcode'],
                        'instant': 10800*((item['current']['Weather']['reference_time']//10800)+1), #shift the weather reference_time to the next closest instant
                        'location': item['current']['Location']['coordinates'],
                        'weather': item['current']['Weather']
                           })
            instants.append(instant['instant'])
#             print(f'INSTANTS = {instants}')
#             print(f'success on try-- _id:{item["_id"]} and zipcode:{item["zipcode"]}---------------')
        except KeyError:
            print(f'caught key error on try-- _id:{item["_id"]} and zipcode:{item["zipcode"]}')
            instant.update({'_id': item['zipcode'],
                        'instant': item['instant'],
                        'location': item['current']['Location']['coordinates'],
                        'weather': item['current']['Weather']
                           })
            instants.append(instant['instant'])
#             print(f'INSTANTS = {instants}')
#             print(f'success on except-- _id:{item["_id"]} and zipcode:{item["zipcode"]}---------------')
    else:
        break

    #     print(f'instants = {instants}')
    col = db.forecast
    forecasts_by_zip = col.find(filters)
    j = 0
    for forecast in forecasts_by_zip:
        cast = forecast['five_day']['weathers']
#         if j<10:
        i = 0
        for cast in cast:
            try:
#                     print('forecast try....')
                inst = instant['instant']
#                     print(f'inst = {inst}')
                ref_time = cast['reference_time']
#                     print(f'ref_time = {ref_time}')
#                 if cast[i]['reference_time'] not in cast[i] and cast[i]['instant'] not in cast[i]:
#                     print('cast[i][reference_time] and cast[i][instant] not in cast[i]')
#                     i+=1
#                     continue
#                     if cast[i]['reference_time'] in cast[i] and 
                if ref_time == inst:
                    instant['forecasts'].append(cast)
                    print('appended to instant[forecast] from ref_time  -----------------------------------------------')
                    i += 1
                    break
#                 elif cast[i]['instant'] in cast[i] and cast[i]['instant'] == instant['instant']:
#                     instant['forecasts'].append(cast[i])
#                     print('appended to instant[forecast]')
#                     i += 1
#                     break
            except KeyError:
                print('checking after exception on cast[instant]...does instant=inst   ...')
#                 print(cast)
                inst = cast['instant']
                if instant == inst-10800:
                    instant['forecasts'].append(cast)
                    print('appended to instant[forecast] from instant  -----------------------------------------------')
                    i += 1
                    break
        j += 1
    try:
        load(instant, client, 'instant')
    except
    n+=1