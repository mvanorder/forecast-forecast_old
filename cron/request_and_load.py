''' This will only get the data from the weather api, make a few edits, and
load it to the local database '''

import os
import json
import time

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError
from pyowm.exceptions.api_call_error import APIInvalidSSLCertificateError

from pymongo import MongoClient
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, InvalidDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

from config import OWM_API_key_loohoo as loohoo_key
from config import OWM_API_key_masta as masta_key
from config import port, host, user, password, socket_path


def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
    :param filename: the name of the file
    :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')

def get_data_from_weather_api(owm, zipcode=None, coords=None):
    ''' Makes api calls for observations and forecasts and handles the API call
    errors.

    :param owm: the OWM API object
    :type owm: pyowm.OWM
    :param zipcode: the zipcode reference for the API call
    :type zipcode: string
    :param coords: the latitude and longitude coordinates reference for the API
    :type coords: 2-tuple 

    returns: the API data
    '''
    result = None
    tries = 1
    while result is None and tries < 4:
        try:
            if coords:
                result = owm.three_hours_forecast_at_coords(**coords)
            elif zipcode:
                result = owm.weather_at_zip_code(zipcode, 'us')
        except APIInvalidSSLCertificateError:
            loc = zipcode or 'lat: {}, lon: {}'.format(coords['lat'], coords['lon'])
            print(f'SSL error with {loc} on attempt {tries} ...trying again')
            if coords:
                owm_loohoo = OWM(loohoo_key)
                owm = owm_loohoo
            elif zipcode:
                owm_masta = OWM(masta_key)
                owm = owm_masta
        except APICallTimeoutError:
            loc = zipcode or 'lat: {}, lon: {}'.format(coords['lat'], coords['lon'])
            print(f'Timeout error with {loc} on attempt {tries}... waiting 1\
                second then trying again')
            time.sleep(1)
        except ValueError:
            try:
                print(result)
            except:
                print('valueError, then exception caught when trying to print\
                    result')
        tries += 1
    if tries == 4:
        print('tried 3 times without response; moving to the next step!')
        return
    return result

def get_current_weather(code=None, coords=None):
    ''' Get the current weather for the given zipcode or coordinates.

    :param code: the zip code to find weather data about
    :type code: string
    :param coords: the coordinates for the data you want
    :type coords: 2-tuple

    :return: the raw weather object
    :type: json
    '''
    owm = OWM(loohoo_key)

    try:
        result = get_data_from_weather_api(owm, zipcode=code)
    except APICallTimeoutError:
        owm = OWM(loohoo_key)
    current = json.loads(result.to_JSON()) # the current weather for the given
                                            # zipcode
    if code:
        current['Weather']['zipcode'] = code
    current['coordinates'] = current['Location']['coordinates']
    current['Weather']['instant'] = 10800*(current['Weather']['reference_time']//10800 + 1)
    current['Weather']['time_to_instant'] = current['Weather']['instant'] - current['Weather'].pop('reference_time')
    current.pop('Location')
    return current

def five_day(coords, code=None):
    ''' Get each weather forecast for the corrosponding coordinates
    
    :param coords: the latitude and longitude for which that that weather is
    being forecasted
    :type coords: tuple containing the latitude and logitude for the forecast

    :return five_day: the five day, every three hours, forecast for the zipcode
    :type five_day: dict
    '''
    owm = OWM(masta_key)

    Forecast = get_data_from_weather_api(owm, coords=coords).get_forecast()
    forecast = json.loads(Forecast.to_JSON())
    if code:
        forecast['zipcode'] = code
    if coords:
        forecast['coordinates'] = coords
    forecast.pop('Location')
    forecast.pop('interval')
    reception_time = forecast['reception_time']  # to add to weathers array
    for cast in forecast['weathers']:
        cast['zipcode'] = forecast['zipcode']
        cast['instant'] = cast.pop('reference_time')
        cast['time_to_instant'] = cast['instant'] - reception_time
    return forecast

def dbncol(client, collection, database='test'):
    ''' Make a connection to the database and collection given in the arguments

    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the name of the database to be used. It must be a database
    name present at the client
    :type database: str
    :param collection: the database collection to be used.  It must be a
    collection name present in the database
    :type collection: str
    
    :return col: the collection to be used
    :type: pymongo.collection.Collection
    '''

    db = Database(client, database)
    col = Collection(db, collection)
    return col

def load_og(data, client, database, collection):
    # Legacy function...see load_weather() for loading needs
    ''' Load data to specified database collection. Also checks for a
    preexisting document with the same instant and zipcode, and updates it in
    the case that there was already one there.

    :param data: the dictionary created from the api calls
    :type data: dict
    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the database to be used
    :type database: str
    :param collection: the database collection to be used
    :type collection: str
    '''
    
    col = dbncol(client, collection, database)
    # create the filters and update types using the data in the dictionary
    if collection == 'instant':
        filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
        updates = {'$push': {'forecasts': data}}    # append the forecast object
                                                    # to the forecasts list
        try:
            # check to see if there is a document that fits the parameters.
            # If there is, update it, if there isn't, upsert it
            col.find_one_and_update(filters, updates, upsert=True)
#             col.find_one_and_update(filters, updates,  upsert=True)
            return
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
    elif collection == 'observed' or collection == 'forecasted':
        try:
            col.insert_one(data)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data into {collection}.')

def load_weather(data, client, database, collection):
    ''' Load data to specified database collection. This determines the
    appropriate way to process the load depending on the collection to which it
    should be loaded. Data is expected to be a weather-type dictionary. When
    the collection is "instants" the data is appended the specified object's
    forecasts array in the instants collection; when the collection is either
    "forecasted" or "observed" the object is insterted uniquely to the
    specified collection. Also checks for a preexisting document with the same
    instant and zipcode, then updates it in the case that there was already
    one there.

    :param data: the dictionary created from the api calls
    :type data: dict
    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the database to be used
    :type database: str
    :param collection: the database collection to be used
    :type collection: str
    ''' 
    col = dbncol(client, collection, database=database)
    # decide how to handle the loading process depending on where the document
    # will be loaded.
    if collection == 'instant' or collection == 'test_instants' or collection == 'instant_temp':
        # set the appropriate database collections, filters and update types
        if "Weather" in data:
            filters = {'zipcode':data['Weather'].pop('zipcode'),
                        'instant':data['Weather'].pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        else:
            filters = {'zipcode':data.pop('zipcode'),
                        'instant':data.pop('instant')}
            updates = {'$push': {'forecasts': data}} # append to forecasts list
        try:
            filters = {'zipcode':data.pop('zipcode'),
                        'instant':data.pop('instant')}
            col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
    elif collection == 'observed'
        or collection == 'forecasted'
        or collection == 'obs_temp'
        or collection == 'cast_temp':
        try:
            col.insert_one(data)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')

def request_and_load(codes):
    ''' Request weather data from the OWM api. Transform and load that data
    into a database.
    
    :param codes: a list of zipcodes
    :type codes: list of five-digit valid strings of US zip codes
    '''
    # Begin a timer for the process and run the request and load process.
    start_start = time.time()
    print(f'task began at {start_start}')
    i, n = 0, 0 # i for counting zipcodes processed and n for counting API
                # calls made; API calls limited to maximum of 60/minute/apikey.
    start_time = time.time()
    for code in codes:
        try:
            current = get_current_weather(code)
        except AttributeError:
            print(f'got AttributeError for {code}. Continuing to next code.')
            continue
        n+=1
        coords = current['coordinates']
        try:
            forecasts = five_day(coords, code=code)
        except AttributeError:
            print(f'got AttributeError for {code}. Continuing to next code.')
            continue
        n+=1
        load_weather(current, local_client, 'owmap', 'obs_temp')
        load_weather(forecasts, local_client, 'owmap', 'cast_temp')
        
        # if the api request rate is greater than 60 just keep going. Otherwise
        # check how many requests have been made and if it's more than 120
        # start make_instants.
        if n/2 / (time.time()-start_time) <= 1:
            print('n/2 / time.time()-start_time <= 1')
            i+=1
            continue
        else:
            print('n/2 / time.time()-start_time > 1')
            i+=1
            if n>20:
                print('about to start making instants')
                import make_instants    # run the file that creates instants
                                        # from the documents just loaded
                if time.time() - start_time < 60:
                    print(f'Waiting {start_time+60 - time.time()} seconds.')
                    time.sleep(start_time - time.time() + 60)
                    start_time = time.time()
    print(f'task took {time.time() -  start_start}sec and processed {i} codes')


if __name__ == '__main__':
    # this try block is to deal with the switching back and forth between
    # computers with different directory names
    try:
        directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    except FileNotFoundError:
        directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    local_client = MongoClient(host=host, port=port)
    request_and_load(codes)
    local_client.close()