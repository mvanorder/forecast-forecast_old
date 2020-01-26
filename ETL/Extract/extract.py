''' This program collects weather data from OpenWeatherMaps API and stores it in a database.
the data collection happens every 3 hours (the granularity of the weather data available).
'''

import json
import os
import time
import urllib
import schedule
import dns

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError, APIInvalidSSLCertificateError
from pprint import pprint
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure
from urllib.parse import quote
from config import OWM_API_key as key, connection_port, user, loc_host, remo_host, password, socket_path

global zipcode
global obs
global reception_time
global zid
global zlon
global zlat
global client
global ref_time
global rec_time

API_key = key
loc_host = loc_host
rem_host = remo_host
port = connection_port
owm = OWM(API_key)    # the OWM object
password = quote(password)    # url encode the password for the mongodb uri
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
print(uri)

def make_zip_list(state):
    ''' Make a list of zip codes in the specified state.
        Read zip codes from downloadable zip codes csv file available at https://www.unitedstateszipcodes.org/
        
        :param state: the two-letter abreviation of the state whose zip codes you'd like listed
        :type state: string
        
        :returns success_zips: list of zip codes that OWM has records for
        :type success_zips: list
    '''
    import pandas as pd
    from pyowm.exceptions.api_response_error import NotFoundError
    
    global owm
    
    successes = 0
    exceptions = 0
    success_zips = []
    fail_zips = []
    
    df = pd.read_csv("resources/zip_code_database.csv")
    # Make a datafram from the rows with the specified state and write the 'zip' column to a list 
    zip_list = df.loc[lambda df: df['state'] == f'{state.upper()}']['zip'].tolist()
    for zipp in zip_list:
        if int(zipp) > 10000:
            try:
                set_location(zipp)
                success_zips.append(zipp)
                successes+=1
                time.sleep(.9)
            except APIInvalidSSLCertificateError:
                print("except", f'APIInvalidSSLCertificateError with zipcode {zipp}...trying again')
                set_location(zipp)
                success_zips.append(zipp)
                successes+=1
                print('this time it worked')
                time.sleep(.9)
            except APIInvalidSSLCertificateError:
                print('same exception again...I will reestablishing the OWM object.')
                owm = OWM(API_key)    # the OWM object
                set_location(zipp)
                success_zips.append(zipp)
                successes+=1
                print('this time it worked')
                time.sleep(.9)
            except APIInvalidSSLCertificateError:
                print('....and again... this time I am just gonna pass.')
                pass
            except NotFoundError:
                print("except", f'NotFoundError with zipcode {zipp}')
                fail_zips.append(zipp)
                exceptions+=1
                pass
    write_list_to_file(success_zips, f'resources/success_zips{state}.csv')
    write_list_to_file(fail_zips, f'resources/fail_zips{state}.csv')
    print(f'successes = {successes}; exceptions = {exceptions}, all written to files')
    return(success_zips)

def write_list_to_file(zip_list, filename):
    """ Write the zip codes to csv file.
        
        :param zip_list: the list created from the zip codes dataframe
        :type zip_list: list stings
        :param filename: the name of the file
        :type filename: sting
    """
    with open(filename, "w") as z_list:
        z_list.write(",".join(str(z) for z in zip_list))
    return
        
def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
        :param filename: the name of the file
        :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')

def set_location(code):
    ''' Get the latitude and longitude corrosponding to the zip code.
        
        :param code: the zip code to find weather data about
        :type code: string
    '''
    global obs, zlat, zlon, owm
    try:
        obs = owm.weather_at_zip_code(f'{code}', 'us')
    except APIInvalidSSLCertificateError:
        print("except", f'APIInvalidSSLCertificateError with zipcode {code}...trying again')
        obs = owm.weather_at_zip_code(f'{code}', 'us')        
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('same exception again...I will reestablishing the OWM object.')
        owm = OWM(API_key)    # the OWM object
        obs = owm.weather_at_zip_code(f'{code}', 'us')
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('....and again... this time I am just gonna return.')
    except APICallTimeoutError:
        time.sleep(5)
        try:
            obs = owm.weather_at_zip_code(f'{code}', 'us')
        except APICallTimeoutError:
            print(f'could not get past the goddamn api call for {code}.')
            return
    location = obs.get_location()
    zlon = location.get_lon()
    zlat = location.get_lat()
    return


def current():
    ''' Dump the current weather to a json

        :return current: the currently observed weather data
        :type current: dict
    '''
    global obs
    global ref_time
    current = json.loads(obs.to_JSON()) # dump all that weather data to a json object
    rec_time = current['reception_time']
    return(current)


def five_day():
    ''' Get each weather forecast for the corrosponding zip code. 
    
        :return five_day: the five day, every three hours, forecast for the zip code
        :type five_day: dict
    '''
    global obs, zlat, zlon, ref_time
    try:
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
    except APIInvalidSSLCertificateError:
        print("except", f'APIInvalidSSLCertificateError ...trying again')
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('same exception again...I will reestablishing the OWM object.')
        owm = OWM(API_key)    # the OWM object
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('....and again... this time I am just gonna return.')
        return()    
    except APICallTimeoutError:
        time.sleep(.5)
        print('caught APICallTimeoutError trying again...')
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
    except APICallTimeoutError:
        print('caught APICallTimeoutError...returning without another try.')
        return(time.time())
    forecast = forecaster.get_forecast()
    return(json.loads(forecast.to_JSON()))


# def get_weather(codes, loc_host, port):
def get_weather(codes, uri):
    ''' Get the weather from the API and load it to the database. 
    
    :param codes: list of zip codes
    :type codes: list of strings
    '''
#     client = check_db_access(loc_host, port)
    client = check_db_access(uri)
    for code in codes:
        # data = {}
        weather = {}
        forecast = {}
        set_location(code)
        weather.update({'_id': time.time(),
                     'zipcode': code,
                     'current': current(),
                    #  'five_day': five_day()
                    })
        load(weather, client)
        forecast.update({'_id': time.time(),
                    #  'zipcode': code,
                    #  'current': current(),
                     'five_day': five_day()
                    })
        load(forecast, client)
    client.close()
    # print('client closed')
    return


# def check_db_access(loc_host, port):
def check_db_access(uri):
    ''' Check the database connection and return the client
    
        :param host: the database host
        :type host: string
        :param port: the database connection port
        :type port: int
        :param uri: the conneciton uri for the remote mongo database
        :type uri: sting
    '''
#     client = MongoClient(host=host, port=port)
    client = MongoClient(uri)    
    try:
        client.admin.command('ismaster')
        # print('client open')
    except ConnectionFailure:
        print("Server not available")
        return
    return(client)


def to_json(data, code):
    ''' Store the collected data as a json file in the case that the database
        is not connected or otherwise unavailable.
        
        :param data: the dictionary created from the api calls
        :type data: dict
        :param code: the zip code associated with the data from the list codes
        :type code: sting
    '''
    collection = data
    directory = os.getcwd()
    save_path = os.path.join(directory, 'Data', f'{code}.json')
    Data = open(save_path, 'a+')
    Data.write(collection)
    Data.close()
    return


def load(data, client):
    ''' Load the data to the database if possible, otherwise write to json file. 
        
        :param data: the dictionary created from the api calls
        :type data: dict
        :param client: the pymongo client object
        :type client: MongoClient
    '''
    if type(data) == dict:
        database = client.OWM
        name = 'code'
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


if __name__ == '__main__':
    directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')
    filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
    codes = read_list_from_file(filename)
    num_zips = len(codes)
    i, n = 0, 0
    print(f'task began at time.time')
    while n < num_zips:
        codeslice = codes[i:i+10]
        i += 10
        n += 10
#         get_weather(codes, loc_host, port)
        get_weather(codeslice, uri)
        time.sleep(10)
