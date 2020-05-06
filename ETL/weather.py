import time

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError
from pyowm.exceptions.api_call_error import APIInvalidSSLCertificateError


class Weather:
    ''' A dictionary of weather variables and their observed/forecasted values
    for a given instant in time at a specified location.
    '''
    
    def __init__(location, _type):
        '''
        :param location: can be either valid US zipcode or coordinate dictionary
        :type location: If this param is a zipcode, it should be str, otherwise dict
        :param _type: Indicates whether its data is observational or forecasted
        :type _type: string  It must be either 'observation' or 'forecast'
        '''
        
        self.time = time.time()//1
        self.type = _type
        if type(location) == str:
            self.loc = location
        elif type(location) == dict:
            self.loc == location
        else:
            print('''I'm trying to instantiate a Weather and you've given something
                  neither string nor dict!''')
        if type(_type) == 'observation':
            self.weather = 
        self.as_dict = {'time': self.time,
                       'location': self.loc,
                       '_type': self.type,
                        'weather': self.weather
                       }
        
    def to_inst(instant):
        ''' This will find the id'd Instant and add the Weather to it according 
        to its type. '''
        

# def get_data_from_weather_api(owm, zipcode=None, coords=None):
def get_data_from_weather_api(owm, location):
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
            if type(location) == dict:
                print('''in get_data_from_weather_api() and wondering if you 
                      really wanted to put a type check for dicts... you may
                      have wanted to use tuple''')
                result = owm.three_hours_forecast_at_coords(**location)
            elif type(location) == str:
                result = owm.weather_at_zip_code(location, 'us')
        except APIInvalidSSLCertificateError:
            loc = zipcode or 'lat: {}, lon: {}'.format(coords['lat'], coords['lon'])
            print(f'SSL error with {loc} on attempt {tries} ...trying again')
            if type(location) == dict:
                owm_loohoo = OWM(loohoo_key)
                owm = owm_loohoo
            elif type(location) == str:
                owm_masta = OWM(masta_key)
                owm = owm_masta
        except APICallTimeoutError:
            loc = location[:] or 'lat: {}, lon: {}'.format(location['lat'], location['lon'])
            print(f'Timeout error with {loc} on attempt {tries}... waiting 1 second then trying again')
            time.sleep(1)
        tries += 1
    if tries == 4:
        print('tried 3 times without response; breaking out and causing an error that will crash your current colleciton process...fix that!')
        return ### sometime write something to keep track of the zip and instant that isn't collected ###
    return result

def get_current_weather(location):
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
            result = get_data_from_weather_api(owm, location)
            current = json.loads(result.to_JSON()) # the current weather for the given zipcode
#             if code:
#                 current['zipcode'] = location
            return current
        except APICallTimeoutError:
            owm = owm_loohoo
            m += 1
    print(f'Did not get current weather for {location} and reset owm')
    return ### after making the weather class, return one of them ###
    
def five_day(location):
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
