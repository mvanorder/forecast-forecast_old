''' Get all the weather data from a list of locations and add those to  their
respective instant document. 
'''

import weather
from pymongo.errors import ServerSelectionTimeoutError

def load_instants_from_db(reverse=False, instants=None, mod=False):
    ''' Pull all the instant collection from the database and load it up to
    a dictionary.
    '''
    from config import client, database
    from Extract.make_instants import find_data
    from db_ops import dbncol

    database = 'OWM'
    collection = 'instant_temp'
    temp = {}  # Holder for the data from database.collection
    data = find_data(client, database, collection)

    try:
        # add each doc to instants and set its key and _id to the same values
        for item in data:
#            print(item)
            # Set the dict keys from the items adding the items to those keys
            if mod == True:
                _id = f'{item.pop("zipcode")}{item.pop("instant")}'
                item['_id'] = _id
                temp[_id] = item
            else:
                temp[f'{item["_id"]}'] = item
    except ServerSelectionTimeoutError as e:
        print(f'Unable to connect to mongodb: {e}')
        exit()
    return temp


if __name__ == '__main__':
    import os
    import time
    
    from Extract.request_and_load import read_list_from_file
    
    print(dir())
    # Get the list of locations from the resources directory
    directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')
    filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
    if not os.path.isfile(filename):
        directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        if not os.path.isfile(filename):
            print(f'{filename} is missing. Pleease create it and populate it with a list of zip codes')
            exit()

    codes = read_list_from_file(filename)
    # Pull in all the documents from the db.instants database collection
    instants = load_instants_from_db()
    # Start pulling all the data from the weather API
    weather_list = []
    
    # Begin a timer for the process and run the request and load process.
    start_start = time.time()
    print(f'task began at {start_start}')
    k, n = 0, 0 # i for counting zipcodes processed and n for counting API
                # calls made; API calls capped at 60/minute/apikey.
    start_time = time.time()
    for code in codes:
        o = weather.get_current_weather(code)  # 'o' for observation
        n += 1
        location = o.weather['Weather'].pop('location')  # You need the coordinate location
                                                         # for five_day().
        weath = o.weather.pop('Weather')
        o.as_dict['weather'] = weath
        weather_list.append(o)
        f = weather.five_day(location)  # 'f' for forecasts
        n += 1
        for item in f:
            weather_list.append(item)

            # if the api request rate is greater than 60 just keep going. Otherwise check how many requests have been made
            # and if it's more than 120 start make_instants.
            if n/2 / (time.time()-start_time) <= 1:
                k+=1
                continue
            else:
                k+=1
                if n>=120:
                    for i in weather_list:
                        i.to_inst(instants)
                    if time.time() - start_time < 60:
                        print(f'Waiting {start_time+60 - time.time()} seconds before resuming API calls.')
                        time.sleep(start_time - time.time() + 60)
                        start_time = time.time()
                    n = 0

    # sort the last of the documents in temp collections
    try:
        for i in weather_list:
            i.to_inst()
#         make_instants(client)
    except:
        print('No more documents to sort into instants')
    print(f'task took {time.time() - start_start} seconds and processed {int(k/40)} zipcodes')
