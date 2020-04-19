import os
import json
import time

# from pyowm import OWM
# from pyowm.weatherapi25.forecast import Forecast
# from pyowm.exceptions.api_response_error import NotFoundError
# from pyowm.exceptions.api_call_error import APICallTimeoutError, APIInvalidSSLCertificateError

from pymongo import MongoClient
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure

from config import OWM_API_key_loohoo as loohoo_key, OWM_API_key_masta as masta_key
from config import port, host

owm_loohoo = OWM(loohoo_key)  # the owm objects for the separate api keys
owm_masta = OWM(masta_key)  # the owm objects for the separate api keys

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