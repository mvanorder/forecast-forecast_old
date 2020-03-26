''' This will only get the data from the weather api, make a few edits, and load it to the local database '''

import os
import json
import time

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError, APIInvalidSSLCertificateError

from pymongo import MongoClient
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure

from config import OWM_API_key_loohoo as loohoo_key, OWM_API_key_masta as masta_key
from config import port, host, user, password, socket_path


owm_loohoo = OWM(loohoo_key)  # the owm objects for the separate api keys
owm_masta = OWM(masta_key)  # the owm objects for the separate api keys
port = port
host = host


def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
    :param filename: the name of the file
    :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')

def get_data_from_weather_api(owm, zipcode=None, coords=None):
    ''' Handle the API call errors for weatehr and forecast type calls.

    :param owm: the OWM API object
    :type owm: pyowm.OWM
    :param zipcode: the zipcode reference for the API call
    :type zipcode: string
    :param coords: the latitude and longitude coordinates reference for the API call
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
            print(f'Timeout error with {loc} on attempt {tries}... waiting 1 second then trying again')
            time.sleep(1)
        tries += 1
    if tries == 4:
        print('tried 3 times without response; breaking out and causing an error that will crash your current colleciton process...fix that!')
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
    global owm_loohoo
    owm = owm_loohoo

    try:
        result = get_data_from_weather_api(owm, zipcode=code)
    except APICallTimeoutError:
        owm
    current = json.loads(result.to_JSON()) # the current weather for the given zipcode
    if code:
        current['zipcode'] = code
    current['coordinates'] = current['Location']['coordinates']
    current['instant'] = 10800*(current['Weather']['reference_time']//10800 + 1)
    current.pop('Location')
    return current

def five_day(code=None, coords=None):
    ''' Get each weather forecast for the corrosponding coordinates
    
    :param coords: the latitude and longitude for which that that weather is being forecasted
    :type coords: tuple containing the latitude and logitude for the forecast

    :return five_day: the five day, every three hours, forecast for the zip code
    :type five_day: dict
    '''
    global owm_masta
    owm = owm_masta

    Forecast = get_data_from_weather_api(owm, coords=coords).get_forecast()
    forecast = json.loads(Forecast.to_JSON())
    if code:
        forecast['zipcode'] = code
    if coords:
        forecast['coordinates'] = coords
    forecast.pop('Location')
    forecast.pop('interval')
    forecast['instant'] = forecast['reception_time']
    for cast in forecast['weathers']:
        cast['instant'] = cast.pop('reference_time')
    return forecast

def load(data, client, database, collection):
    ''' Load data to specified database collection. Also checks for a preexisting document with the same instant and zipcode, and updates
    it in the case that there was already one there.

    :param data: the dictionary created from the api calls
    :type data: dict
    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the database to be used
    :type database: str
    :param collection: the database collection to be used
    :type collection: str
    '''
    
    filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
    updates = {'$set': data} 
    try:
        db = Database(client, database)
        col = Collection(db, collection)
        # check to see if there is a document that fits the parameters. If there is, update it, if there isn't, upsert it
        col.find_one_and_update(filters, updates,  upsert=True)
    except DuplicateKeyError:
        return(f'DuplicateKeyError, could not insert data into {name}.')


if __name__ == '__main__':
    # Try block to deal with the switching back anc forth between computers with different directory names
    try:
        directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    except FileNotFoundError:
        directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    codes = read_list_from_file(filename)
    num_zips = len(codes)
    start_start = time.time()
    print(f'task began at {start_start}')
    local_client = MongoClient(host=host, port=port)
    start_time = time.time()
    i, n = 0, 0 #i for coundting zipcodes processed and n for counting API calls made; API calss limited to a maximum of 60/minute.
    for code in codes:
        try:
            current = get_current_weather(code)
        except AttributeError:
            print(f'got AttributeError while collecting current weather for {code}. Continuing to next code.')
            continue
        n+=1
        load(current, local_client, 'test', 'observed')
        coords = current['coordinates']
        try:
            forecasts = five_day(code, coords=coords)
        except AttributeError:
            print(f'got AttributeError while collecting forecasts for {code}. Continuing to next code.')
            continue
        n+=1
        load(forecasts, local_client, 'test', 'forecasted')
        # Wait for the next 60 seconds to resume making API calls
        if n==120 and time.time()-start_time <= 60:
            print(f'Waiting {60 - time.time() + start_time} seconds before resuming API calls.')
            time.sleep(60 - time.time() + start_time)
            start_time = time.time()
            n = 0
        i+=1
    local_client.close()
    print(f'task took {time.time() -  start_start} seconds and processed {i} zipcodes')