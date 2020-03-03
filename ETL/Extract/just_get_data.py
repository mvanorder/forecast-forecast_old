''' This will get only the data from the weather api and load it to the approriate database '''

import os
import json
import time
from urllib.parse import quote

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError, APIInvalidSSLCertificateError

from pymongo import MongoClient
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure

from config import OWM_API_key_masta as masta_key, OWM_API_ky_loohoo as loohoo_key, port, local_host


masta_owm = OWM(masta_key)  # the owm objects for the separate api keys
loohoo_owm = OWM(loohoo_key)
print(type(masta_owm))
print(type(loohoo_owm))


def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
    :param filename: the name of the file
    :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')

def get_current_weather(code=None, coords=None):
    ''' Get the current weather for the given zipcode or coordinates.

    :param code: the zip code to find weather data about
    :type code: string
    :param coords: the coordinates for the data you want
    :type coords: 2-tuple

    :return: the raw weather object
    :type: json
    '''
    global owm
    
    obs = get_data_from_weather_api(owm, 'weather', zipcode=str(code))
    current = json.loads(obs.to_JSON()) # the current weather for the given zipcode
    return current

def five_day(coords=None):
    ''' Get each weather forecast for the corrosponding coordinates
    
    :param coords: the latitude and longitude for which that that weather is being forecasted
    :type coords: tuple containing the latitude and logitude for the forecast

    :return five_day: the five day, every three hours, forecast for the zip code
    :type five_day: dict
    '''
    global owm

    forecaster = get_data_from_weather_api(owm, 'forecast', coords=(zlat, zlon))
    forecast = forecaster.get_forecast()
    return json.loads(forecast.to_JSON())
        
def load(data, database, collection):
    ''' Add the observed weather to the corrosponding instant document and load it to the remote database 
        
    :param data: the dictionary created from the api calls
    :type data: dict
    :param database: the datase that the data is supposed to go to
    :type database: pymong.database.Databse
    :param collection: the database collection to be used
    :type collection: pymongo.collection.Collection
    '''
### you need to find out what is in the raw data so that you can find what you wantuse to 
    filters = {maybe the reference time}  #{'zipcode':data['zipcode'], 'instant':data['instant']}
    updates = {'$set': data} # use only the weather object from the current weather created from the API call
    try:
        # check to see if there is a document that fits the parameters. If there is, update it, if there isn't, upsert it
        updates = col.find_one_and_update(filters, updates,  upsert=True, return_document=ReturnDocument.AFTER)
        col = Collection(database, collection)
        loaded = col.update_many(updates)
    except DuplicateKeyError:
        return(f'DuplicateKeyError, could not insert data into {name}.')
    

if __name__ == '__main__':
    try:
        directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
    except FileNotFoundError:
        print('caught filenotfounderror, trying forcast-forcast')
        directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')
        filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
        codes = read_list_from_file(filename)
        print('Got it')
    codes = read_list_from_file(filename)
    num_zips = len(codes)
    i, n = 0, 0
    print(f'task began at {time.localtime()}')
    client = MongoClient(uri)
    for code in codes:
        print(f'processing th {n}th')
        current = set_location_and_get_current(code)
        zlat = current['location']['lat']
        zlon = current['location']['lon']
        forecasts = five_day(zlat, zlon)
        sort_casts(forecasts, code, client)
        load(current, client, 'instant')
        n+=1
    client.close()
    print(f'task ended at {time.localtime()} and processed like {n} zipcodes')
