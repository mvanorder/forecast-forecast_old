''' This program collects weather data from OpenWeatherMaps API and stores it in a database.
the data collection happens every 3 hours (the granularity of the weather data available).
'''

import json
import os
import time
import urllib
import schedule
import dns
import pandas as pd

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError, APIInvalidSSLCertificateError
from pymongo import MongoClient
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure
from urllib.parse import quote
from config import OWM_API_key as key, connection_port, user, password, socket_path

zipcode = str
zlon = float
zlat = float
client = MongoClient()

API_key = key
# loc_host = loc_host
# rem_host = remo_host
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
                print(f'except first try: APIInvalidSSLCertificateError with zipcode {zipp}...trying again')
                set_location(zipp)
                success_zips.append(zipp)
                successes+=1
                print('this time it worked')
                time.sleep(.9)
            except APIInvalidSSLCertificateError:
                print('except on second try: APIInvalidSSLCertificateError - reestablishing the OWM object and trying again.')
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
        print(f'except first try in set_location(): APIInvalidSSLCertificateError with zipcode {code}...trying again')
        obs = owm.weather_at_zip_code(f'{code}', 'us')        
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('except on second try in set_location(): APIInvalidSSLCertificateError - reestablishing the OWM object and trying again.')
        owm = OWM(API_key)    # the OWM object
        obs = owm.weather_at_zip_code(f'{code}', 'us')
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('....and again... this time I am just gonna return.')
        return
    except APICallTimeoutError:
        time.sleep(5)
        try:
            print('caught APICallTimeoutError in set_location()')
            obs = owm.weather_at_zip_code(f'{code}', 'us')
        except APICallTimeoutError:
            print(f'could not get past the goddamn api call for {code}.')
            return
    location = obs.get_location()
    zlon = location.get_lon()
    zlat = location.get_lat()
    return({'lat': zlat,
            'lon': zlon
            })

def current():
    ''' Dump the current weather to a json

        :return current: the currently observed weather data
        :type current: dict
    '''
    global obs
    current = json.loads(obs.to_JSON()) # dump all that weather data to a json object
    return(current)

def five_day():
    ''' Get each weather forecast for the corrosponding zip code. 
    
        :return five_day: the five day, every three hours, forecast for the zip code
        :type five_day: dict
    '''
    
    global obs, zlat, zlon, owm
    
    try:
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
    except APIInvalidSSLCertificateError:
        print(f'except on first try in firve_day(): APIInvalidSSLCertificateError ...trying again')
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('except on second try five_day(): APIInvalidSSLCertificateReeor... reestablish the OWM object and try again.')
        owm = OWM(API_key)    # the OWM object
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('....and again... this time I am just gonna return.')
        return
    except APICallTimeoutError:
        time.sleep(.5)
        print('caught APICallTimeoutError in five_day(). trying again...')
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
    except APICallTimeoutError:
        print('caught APICallTimeoutError in five_day()...returning without another try.')
        return(time.time())
    forecast = forecaster.get_forecast().to_JSON()
    forecast = json.loads(forecast)
    forecasts = forecast['weathers']
    return(forecasts)

def insert_instant(weather, code, client, location):
    ''' Insert anew instant object to the database

        :param weather: the weather object returned by the current() function
        :type weather: json object
        :param code: the zipcode of the instant
        :type code: string
        :param client: the mongodb client
        :type client: pymongo.MongoClient
    '''
    instant = {'last_update': time.time(),
                'zipcode': code,
                'instant': 10800*(weather['reference_time']//10800 + 1), # set the instant to the next reference instant
                'location': location
                }
    db = client.test
    col = db.instant
    try:
        col.insert_one(instant)
    except:
        print(f'exception inserting instant at {code} and {instant["instant"]}')
    return


def sort_casts(forecasts, code, client):
    ''' Take the array of forecasts from the five day forecast and sort them into the documents of the instants collection.
        
        :param forecasts: the forecasts from five_day()-  They come in a list of 40, one for each of every thrid hour over five days
        :type forecasts: list- expecting a list of forecasts
        :param code: the zipcode
        :type code: string
        :param client: the mongodb client
        :type client: MongoClient
    '''
    db = client.test
    col = db.instant
    for forecast in forecasts:
        # filter out the unneeded data  ##### I should have popped out the stuff I don't need
        forecast = {'reception_time': time.time(),
              'reference_time': forecast['reference_time'],
              'clouds': forecast['clouds'],
              'rain': forecast['rain'],
              'snow': forecast['snow'],
              'wind': forecast['wind'],
              'humidity': forecast['humidity'],
              'pressure': forecast['pressure'],
              'tempurature': forecast['temperature'],
              'status': forecast['status'],
              'detailed_status': forecast['detailed_status'],
              'weather_code': forecast['weather_code'],
              'dewpint': forecast['dewpoint'],
              'humidex': forecast['humidex'],
              'heat_index': forecast['heat_index']}
        # now find the document that has that code and that ref_time
        # This should change to the instants collection, find a singel instant specified by zip and the
        # forecast ref_time, and finally append the forecast to the forecasts object
        filter_by_zip_and_inst = {'zipcode':code, 'instant':forecast['reference_time']}
        filters = filter_by_zip_and_inst
        add_forecast_to_instant = {'$push': {'forecasts': forecast}}
        updates = add_forecast_to_instant
        col.find_one_and_update(filters, updates, upsert=True, return_document=ReturnDocument.AFTER)

def insert_weather(weather, code, client):
    ''' Insert anew instant object to the database

        :param weather: the weather object returned by the current() function
        :type weather: json object
        :param code: the zipcode of the instant
        :type code: string
        :param client: the mongodb client
        :type client: pymongo.MongoClient
    '''
    db = client.test
    col = db.instant
    filter_by_zip_and_inst = {'zipcode':code, 'instant':weather['reference_time']}
    filters = filter_by_zip_and_inst
    add_weather_to_instant = {"$set": { 'weather': weather}}
    updates = add_weather_to_instant
    col.find_one_and_update(filters, updates, upsert=True, return_document=ReturnDocument.AFTER)

# def get_weather(codes, loc_host, port):
def get_weather(codes, uri):
    ''' Get the weather from the API and load it to the database. 
    
    :param codes: list of zip codes
    :type codes: list of strings
    :param uri: the uri for the database connection
    :type uri: string
    '''
#     client = check_db_access(loc_host, port)
    client = check_db_access(uri)
    try:
        db = client.test
    except AttributeError:
        print('maybe did not make client connection...trying again.')
        time.sleep(.5)
        client = check_db_access(uri)
        db = client.test
        print('must have worked')
    # instant = {}
    # weather = {}
    # forecast = {}
    for code in codes:
        location = set_location(code)
        Current = current() # returns json object
        forecasts = five_day() # list of json objects
        # create your weather object from the OWM weather object
        weather = {'reference_time': Current['Weather']['reference_time'],
                  'clouds': Current['Weather']['clouds'],
                  'rain': Current['Weather']['rain'],
                  'snow': Current['Weather']['snow'],
                  'wind': Current['Weather']['wind'],
                  'humidity': Current['Weather']['humidity'],
                  'pressure': Current['Weather']['pressure'],
                  'tempurature': Current['Weather']['temperature'],
                  'status': Current['Weather']['status'],
                  'detailed_status': Current['Weather']['detailed_status'],
                  'weather_code': Current['Weather']['weather_code'],
                  'dewpint': Current['Weather']['dewpoint'],
                  'humidex': Current['Weather']['humidex'],
                  'heat_index': Current['Weather']['heat_index']
                  }
        insert_instant(weather, code, client, location)
        sort_casts(forecasts, code, client)
        insert_weather(weather, code,client)
        # weather.update({'instant': instant['instant'],
        #                 'reception_time': time.time(),
        #                 'zipcode': code,
        #                 'current': Current
        #             })
        # load(weather, client, 'weather')
        # forecast.update({'instant': instant['instant'],
        #                  'reception_time': time.time(),
        #                  'zipcode': code,
        #                  'five_day': five_day()
        #             })
        # load(forecast, client, 'forecast')
    client.close()
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

def load(data, client, name):
    ''' Load the data to the database if possible, otherwise write to json file. 
        
        :param data: the dictionary created from the api calls
        :type data: dict
        :param client: the pymongo client object
        :type client: MongoClient
        :param name: the database collection to be used
        :type name: 
    '''
    insert_record = []
    if type(data) == dict:
        database = client.test
        col = Collection(database, name)
        filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
        updates = {'$setOnInsert': data}
        try:
            # check to see if there is a document that fits the parameters. If there is, update it, if there isn't, upsert it
            update = col.find_one_and_update(filters, updates,  upsert=True, return_document=ReturnDocument.BEFORE)
            if update == None:
                return
        except DuplicateKeyError:
            print(f'DuplicateKeyError, could not insert to {name}')
    else:
        print('data is coming into load() not as a dict')
        client.close()
        print('closed db connection')
    return


if __name__ == '__main__':
    # directory = os.path.join(os.environ['HOME'], 'data', 'forecast-forecast')  # for macbook pro
    directory = os.path.join(os.environ['HOME'], 'data', 'forcast-forcast')  # for macbook air
    filename = os.path.join(directory, 'ETL', 'Extract', 'resources', 'success_zipsNC.csv')
    codes = read_list_from_file(filename)
    num_zips = len(codes)
    i, n = 0, 0
    print(f'task began at {time.localtime()}')
    client = MongoClient(uri)
    db = client.test
    instant = db.instant
    db.drop_collection(instant)
    while n < 10: #num_zips:
        codeslice = codes[i:i+10]
        i += 10
        n += 10
    #         get_weather(codes, loc_host, port)
        get_weather(codeslice, uri)
#         time.sleep(10)
    client.close()
    print(f'task ended at {time.localtime()}')

