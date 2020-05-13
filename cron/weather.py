''' Defines the Weather class and related functions. '''

import time
import json

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError
from pyowm.exceptions.api_call_error import APIInvalidSSLCertificateError

from config import OWM_API_key_loohoo as loohoo_key
from config import OWM_API_key_masta as masta_key
from instant import Instant


class Weather:
    ''' A dictionary of weather variables and their observed/forecasted values
    for a given instant in time at a specified location.
    '''
    
    def __init__(self, location, _type, data=None):
        '''
        :param location: can be either valid US zipcode or coordinate dictionary
        :type location: If this param is a zipcode, it should be str, otherwise
        dict
        :param _type: Indicates whether its data is observational or forecasted
        :type _type: string  It must be either 'observation' or 'forecast'
        '''

        self.type = _type
        self.loc = location
        self.weather = data
        # make the _id for each weather according to its reference time
        if _type == 'forecast' and 'reference_time' in data:
            self._id = f'{str(location)}{str(data["reference_time"])}'
        elif _type == 'observation' and 'Weather' in data:
            self._id = f'{str(location)}{str(10800 * (data["Weather"]["reference_time"]//10800 + 1))}'
        self.as_dict = {'_id': self._id,
                       '_type': self.type,
                        'weather': self.weather
                       }

    def to_inst(self, instants):
        ''' This will find the id'd Instant and add the Weather to it according 
        to its type.
        
        :param instants: a collection of instants
        :type instnats: dict
        
        *** NOTE: the object instants must be in the function's namespace ***
        '''

        if not instants:
            instants = {'init': 'true'}
        if self.type == 'observation':
            instants.setdefault(self._id, Instant(self._id, observations=self.weather))
            return
        if self.type == 'forecast':
            instants.setdefault(self._id, Instant(self._id)).casts.append(self.weather)
            return


def get_data_from_weather_api(owm, location):
    ''' Makes api calls for observations and forecasts and handles the API call
    errors.

    :param owm: the OWM API object
    :type owm: pyowm.OWM
    :param location: the coordinates or zipcode reference for the API call.
    :type location: if location is a zipcode, then type is a string;
    if location is a coordinates, then tuple or dict.

    returns: the API data
    '''
    
    result = None
    tries = 1
    while result is None and tries < 4:
        try:
            if type(location) == dict:
                result = owm.three_hours_forecast_at_coords(**location)
                return result
            elif type(location) == str:
                result = owm.weather_at_zip_code(location, 'us')
                return result
        except APIInvalidSSLCertificateError as e:
            print(str(e))
            if type(location) == dict:
                loc = 'lat: {}, lon: {}'.format(location['lat'], location['lon'])
                owm_loohoo = OWM(loohoo_key)
                owm = owm_loohoo
            elif type(location) == str:
                loc = location
                owm_masta = OWM(masta_key)
                owm = owm_masta
            print(f'SSL error with {loc} on attempt {tries} ...trying again')
        except APICallTimeoutError:
            loc = location or 'lat: {}, lon: {}'.format(location['lat'],
                                                           location['lon'])
            print(f'''Timeout error with {loc} on attempt {tries}... waiting 1
                  second then trying again''')
            time.sleep(1)
        tries += 1
    if tries == 4:
        print('''tried 3 times without response; breaking out and causing an
        error that will crash your current colleciton process...fix that!''')
        return -1  ### sometime write something to keep track of the zip and
                ### instant that isn't collected ###

def get_current_weather(location):
    ''' Get the current weather for the given zipcode or coordinates.

    :param location: the coordinates or zipcode reference for the API call.
    :type location: if location is a zipcode, then type is a string;
    if location is a coordinates, then tuple or dict.

    :return: the raw weather object
    :type: json
    '''
    owm = OWM(loohoo_key)

    m = 0
    # Try several times to get complete the API request
    while m < 4:
        try:
            # get the raw data from the OWM and make a Weather from it
            result = get_data_from_weather_api(owm, location)
            if result is -1:
                print(f'Did not get current weather for {location}; reset owm')
                return result
            result = json.loads(result.to_JSON())   # the current weather for
                                                    # the given zipcode
            result['Weather']['location'] = result['Location'].pop('coordinates')
            result.pop('reception_time')
            result.pop('Location')
            weather = Weather(location, 'observation', result)
            return weather
        except APICallTimeoutError:
            owm = owm_loohoo
            m += 1
    
def five_day(location):
    ''' Get each weather forecast for the corrosponding coordinates
    
    :param coords: the latitude and longitude for which that that weather is
    being forecasted
    :type coords: tuple containing the latitude and logitude for the forecast

    :return casts: the five day, every three hours, forecast for the zip code
    :type casts: list of Weather objects
    '''

    owm = OWM(masta_key)

    Forecast = get_data_from_weather_api(owm, location).get_forecast()
    forecast = json.loads(Forecast.to_JSON())
    casts = [] # This is for the weather objects created in the for loop below.
    for data in forecast['weathers']:
        # Make an _id for the next Weather to be created, create the weather, 
        # append it to the casts list.
        instant = data['reference_time']
        casts.append(Weather(location, 'forecast', data))
    return casts
