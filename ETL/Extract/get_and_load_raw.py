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
    ''' Makes api calls for observations and forecasts and handles the API call errors.

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
        return ### sometime write something to keep track of the zip and instant that isn't collected ###
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

    m = 0
    while m < 4:
        try:
            result = get_data_from_weather_api(owm, zipcode=code)
            current = json.loads(result.to_JSON()) # the current weather for the given zipcode
            if code:
                current['zipcode'] = code
            return current
        except APICallTimeoutError:
            owm = owm_loohoo
            m += 1
    print(f'Did not get current weather for {code} and reset owm')
    return ### after making the weather class, return one of them ###
    
def five_day(coords, code=None):
    ''' Get each weather forecast for the corrosponding coordinates
    
    :param coords: the latitude and longitude for which that that weather is being forecasted
    :type coords: tuple containing the latitude and logitude for the forecast

    :return five_day: the five day, every three hours, forecast for the zip code
    :type five_day: dict
    '''
    owm = OWM(masta_key)

    Forecast = get_data_from_weather_api(owm, coords=coords).get_forecast()
    forecast = json.loads(Forecast.to_JSON())
    if codes:
        forecast['zipcode'] = code
    return forecast

def dbncol(client, collection, database=None):
    ''' Make a connection to the database and collection given in the arguments.

    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the name of the database to be used. It must be a database name present at the client
    :type database: str
    :param collection: the database collection to be used.  It must be a collection name present in the database
    :type collection: str
    
    :return col: the collection to be used
    :type: pymongo.collection.Collection
    '''

    if database ==  None:
        database = 'test'
    db = Database(client, database)
    col = Collection(db, collection)
    return col

def request_and_load(codes):
    ''' Request weather data from the OWM api. Transform and load that data into a database.
    
    :param codes: a list of zipcodes
    :type codes: list of five-digit valid strings of US zip codes
    '''
    # Begin a timer for the process and run the request and load process.
    start_start = time.time()
    print(f'task began at {start_start}')
    i, n = 0, 0 # i for counting zipcodes processed and n for counting API calls made; API calls limited to a maximum of 60/minute/apikey.
    start_time = time.time()
    currents_list = []
    forecasts_list = []
    for code in codes:
        try:
            current = get_current_weather(code)
            currents_list.append(current)
            coords = current['Location']['coordinates']
        except AttributeError:
            print(f'got AttributeError while collecting current weather for {code}. Continuing to next code.')
            continue
        n+=1
        # load_weather(current, local_client, 'test', 'observed')
        try:
            forecasts = five_day(coords, code)
            forecasts_list.append(forecasts)
        except AttributeError:
            print(f'got AttributeError while collecting forecasts for {code}. Continuing to next code.')
            continue
        n+=1
        # load_weather(forecasts, local_client, 'test', 'forecasted')
        # Wait for the next 60 second interval to resume making API calls
        if n==120 and time.time()-start_time <= 60:
            col = dbncol(client, 'observed')            
            col.insert_many(currents_list)
            col = dbncol(client, 'forecasted')            
            col.insert_many(forecasts_list)
            print(f'Waiting {60 - time.time() + start_time} seconds before resuming API calls.')
            time.sleep(60 - time.time() + start_time)
            start_time = time.time()
            currents_list = []
            forecasts_list = []
            n = 0
        i+=1
    # insert whatever is left in the lists into their databases
    col = dbncol(client, 'observed')            
    col.insert_many(currents_list)
    col = dbncol(client, 'forecasted')            
    col.insert_many(forecasts_list)
    print(f'task took {time.time() -  start_start} seconds and processed {i} zipcodes')


if __name__ == '__main__':
    try:
        directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    except FileNotFoundError:
        directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    client = MongoClient(host=host, port=port)
    # database = 'OWM'
    request_and_load(codes[:80])
    client.close()