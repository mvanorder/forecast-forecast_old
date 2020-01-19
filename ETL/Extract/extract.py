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
from pyowm.exceptions.api_call_error import APICallTimeoutError
from pprint import pprint
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure
from urllib.parse import quote
from config import OWM_API_key as key, connection_port, user, loc_host, remo_host, password, socket_path

API_key = key
loc_host = loc_host
rem_host = remo_host
port = connection_port
filename = 'resources/zip_list.csv'
password = quote(password)  # url code the password
uri = "mongodb+srv://%s:%s@%stest?retryWrites=true&w=majority" % (user, password, socket_path)

global zipcode
owm = OWM(API_key) # the OWM object
global obs
global reception_time
global zid
global zlon
global zlat
global client


def make_zip_list(state):
#     print('using make_zip_list')
    ''' Make a list of zip codes in the specified state.
        Read zip codes from downloadable zip codes csv file available at https://www.unitedstateszipcodes.org/
        
        :param state: the two-letter abreviation of the state whose zip codes you'd like listed
        :type state: string
        
        :returns success_zips: list of zip codes that OWM has records for
        :type success_zips: list
    '''
    import pandas as pd
    from pyowm.exceptions.api_response_error import NotFoundError
    
    successes = 0
    exceptions = 0
    success_zips = []
    fail_zips = []
    
    df = pd.read_csv("resources/zip_code_database.csv")
    # Make a datafram from the rows with the specified state and write the 'zip' column to a list 
    zip_list = df.loc[lambda df: df['state'] == f'{state.upper()}']['zip'].tolist()
#     zip_list = df['zip'].tolist()
    for zipp in zip_list:
        if int(zipp) > 10000:
            try:
#                 print('try setting location for ', zipp)
                set_location(zipp)
#                 print(f'successfully set location for {zipp}', len(success_zips))
                success_zips.append(zipp)
                successes+=1
            except NotFoundError:
                print("except", f'NotFoundError with zipcode {zipp}')
                fail_zips.append(zipp)
                exceptions+=1
                pass
    write_list_to_file(success_zips, 'resources/success_zips.csv')
    write_list_to_file(fail_zips, 'resources/fail_zips.csv')
    print(f'successes = {successes}; exceptions = {exceptions}, all written to files')
    return(success_zips)

def write_list_to_file(zip_list, filename):
#     print('using write_list_to_file')
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
#     print('using read_list_from_file')
    """ Read the zip codes list from the csv file.
        
        :param filename: the name of the file
        :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')


def set_location(code):
    print(f'using set_location for {code}')
    ''' Get the latitude and longitude corrosponding to the zip code.
        
        :param code: the zip code to find weather data about
        :type code: string
        
        :return pass: hopefully this will get get_weather() to skip the zip if it can't get the api to respond
    '''
    global obs, zlat, zlon
    print(f'the zip code is {code}, and I am trying to put it into owm.weather_at_zip function.')
    try:
        n+=1
        if n == 4:
            print('done tried that 3 times, time to figure something else out...')
            return(False)
        time.sleep(.5)
        obs = owm.weather_at_zip_code(f'{code}', 'us')
    except APICallTimeoutError as e:
        print(e, 'seeing if I need to reinitialize the OWM object')
        owm = OWM(API_key) # try to reestablish the OWM object
        set_location(code)
#        time.sleep(2)
#         try:
#             obs = owm.weather_at_zip_code(f'{code}', 'us')
#         except APICallTimeoutError:
#             print(f'\n******************************************{code}*************************************************\n')
#             pass
# #             return(f'did not get location set for {code})
    location = obs.get_location()
    zlon = location.get_lon()
    zlat = location.get_lat()
    return(True)


def current():
#     print('using current')
    ''' Dump the current weather to a json

        :return current: the currently observed weather data
        :type current: dict
    '''
    global obs
    current = json.loads(obs.to_JSON()) # dump all that weather data to a json object
    return(current)


def five_day():
#     print('using five_day')
    ''' Get each weather forecast for the corrosponding zip code. 
    
        :return five_day: the five day, every three hours, forecast for the zip code
        :type five_day: dict
    '''
    global obs, zlat, zlon
    try:
        forecaster = owm.three_hours_forecast_at_coords(zlat, zlon)
    except APICallTimeoutError:
        time.sleep(.5)
        print('caught APICallTimeoutError')
        return(time.time())
    forecast = forecaster.get_forecast()
    return(json.loads(forecast.to_JSON()))


def get_weather(codes, uri):
    print(f'using get_weather for {code}', time.time())
    ''' Get the weather from the API and load it to the database. 
    
    :param codes: list of zip codes
    :type codes: list of strings
    '''
    client = check_db_access(uri)
    for code in codes:
        n = 0 # count the number of times set_location() is called- it will call itself if it encounters APIcalltimeouterror
        data = {}
        if set_location(code) is False:
            pass
        time.sleep(1)
        data.update({'zipcode': code,
                     'current': current(),
                     'five_day': five_day(),
                    })
        load(data, client)
#         print(f'data in for {code}')
    client.close()
    print('client closed')
    return()


# def check_db_access(host, port):
def check_db_access(uri):
    print('using check_db_access')
    ''' A check that there is write access to the database
    
        :param host: the database host
        :type host: string
        :param port: the database connection port
        :type port: int
    '''
#     client = MongoClient(host=host, port=port)
    client = MongoClient(uri)    
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print('client open')
    except ConnectionFailure:
        print("Server not available")
        return

    # check the database connections
        # Get a count of the number of databases at the connection (accessible through that port)
        # before attempting to add to it
    db_count_pre = len(client.list_database_names())
        # Add a database and collection
    db = client.test_db
    col = db.test_col

    # Insert something to the db
    post = {'name':'Chuck VanHoff',
           'age':'38',
           'hobby':'gardening'
           }
    col.insert_one(post)

        # Get a count of the databases after adding one
    db_count_post = len(client.list_database_names())

    if db_count_pre-db_count_post>=0:
        print('Your conneciton is flipped up')
    else:
        print('You have write access')

    client.drop_database(db)
    return(client)


def to_json(data, code):
#     print('using to_json')
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
#     print('using load')
    ''' Load the data to the database if possible, otherwise write to json file. 
        
        :param data: the dictionary created from the api calls
        :type data: dict
        :param client: the pymongo client object
        :type client: MongoClient
    '''
    if type(data) == dict:
        try:
            db = client.forcast
            col = db.code
            col.insert_one(data)
#             print(f'inserted data for {data["zipcode"]}')
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


def scheduled_forecast_request():
    ''' This function is going to make a forecast request every three hours as long
        as it's running. 
    
        :no params:
        :no returns:
    '''
    start_time = time.time()
    n = 0
    
    schedule.every(3).hours.do(get_weather, codes, uri).run()
    while True:
        n+=1
        print(f'collected forecast data {n} times, and I been doing this for {(time.time()-start_time)//60} minutes.')
        schedule.run_pending()
        time.sleep(time.time()-start_time)
        
if __name__ == '__main__':
    filename = 'resources/success_zips.csv'
    codes = read_list_from_file(filename)
#     get_weather(codes, uri)
    scheduled_forecast_request()
