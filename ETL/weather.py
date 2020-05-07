''' Defines the Weather class. '''

import time


class Weather:
    ''' A dictionary of weather variables and their observed/forecasted values
    for a given instant in time at a specified location.
    '''
    
    def __init__(location, _type, data=None):
        '''
        :param location: can be either valid US zipcode or coordinate dictionary
        :type location: If this param is a zipcode, it should be str, otherwise dict
        :param _type: Indicates whether its data is observational or forecasted
        :type _type: string  It must be either 'observation' or 'forecast'
        '''
     
        self.time = time.time() // 1
        self._id = f'{str(location)}{str(self.time)}'
        self.type = _type
        self.loc = location
        self.weather = data
        # if type(_type) == 'observation' and get == True:
        #     self.weather = get_current_weather(location)
        # if type(_type) == 'forecast' and get == True:
        #     self.weather = five_day(location)
        self.as_dict = {'_id': self.time,
                       '_type': self.type,
                        'weather': self.weather
                       }
        
    def to_inst(self):
        ''' This will find the id'd Instant and add the Weather to it according 
        to its type. '''
        
        weather = self.as_dict
        if self.type == 'observation':
            _id = self._id
            instants[_id]['observation'] = weather
            return
        if self.type == 'forecast':
            _id = self._id
            instants[_id]['forecasts'].append(weather)
            return


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

    :return forecast: the five day, every three hours, forecast for the zip code
    :type forecast: dict
    '''

    owm = OWM(masta_key)

    Forecast = get_data_from_weather_api(owm, location).get_forecast()
    forecast = json.loads(Forecast.to_JSON())
    if codes:
        forecast['zipcode'] = code
    return forecast
