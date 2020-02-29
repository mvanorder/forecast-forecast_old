''' functions to keep handle errors from  API calls and database interacitons '''

import time

from pyowm import OWM
from pyowm.weatherapi25.forecast import Forecast
from pyowm.exceptions.api_response_error import NotFoundError
from pyowm.exceptions.api_call_error import APICallTimeoutError, APIInvalidSSLCertificateError


def get_data_from_weather_api(owm, request_type, zipcode=None, coords=None):
    ''' Handle the API call errors for weatehr and forecast type calls.

    :param owm: the OWM API object
    :type owm: pyowm.OWM
    :param request_type: should be either 'forecast' or 'weather' to direct the API calls
    :type request_type: string
    :param zipcode: the zipcode reference for the API call
    :type zipcode: string
    :param coords: the latitude and longitude coordinates reference for the API call
    :type coords: 2-tuple

    returns: the API data
    '''
    
    result = None
    try:
        if coords:
            result = owm.three_hours_forecast_at_coords(*coords)
        elif zipcode:
            result = owm.weather_at_zip_code(zipcode, 'us')
    except APIInvalidSSLCertificateError:
        ssl_err(owm, which)
    except APICallTimeoutError:
        timeout_err(owm, which)
    return result


def ssl_err(owm, which):
    ''' handle the APIInvalidSSLCertificateError from the pyowm module by try and try again method 

    :param owm: the pyowm connection object
    :type owm: OWM API certificate
    :param which: specify whether the command is for a weather call or a forecast call
    :type which: string
    '''
    global code, zlat, zlon
    print(f'except first try in set_location(): APIInvalidSSLCertificateError with zipcode {code}...trying again')
    try:
        # switch between calls to the API depending on the stage of extraction: either weather or forecast
        if which == 'weather':
            obs = owm.weather_at_zip_code(f'{code}', 'us')
            print('thins time it worked')
        elif which == 'forecast':
            obs = owm.three_hours_forecast_at_coords(zlat, zlon)
            print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('except on second try in set_location(): APIInvalidSSLCertificateError - reestablishing the OWM object and trying again.')
        try:
            owm = OWM(API_key)    # the OWM object
            if which ==  'weather':
                obs = owm.weather_at_zip_code(f'{code}', 'us')
                print('this time it worked')
            elif which == 'forecast':
                obs = three_hours_forecast_at_coords(zlat, zlon)
                print('this time it worked')
        except APIInvalidSSLCertificateError:
            print('....and again... this time I am just gonna return.')
            return(f'the time is {time.time()}')

def timeout_err(owm, which):
    ''' handle the APIInvalidSSLCertificateError from the pyowm module by try and try again method 

    :param owm: the pyowm connection object
    :type owm: OWM API certificate
    :param which: specify whether the command is for a weather call or a forecast call
    :type which: string    
    '''
    global code, zlat, zlon
    print('caught APICallTimeoutError on first try in set_location()...trying again')
    time.sleep(5)
    try:
        if which ==  'weather':
            obs = owm.weather_at_zip_code(f'{code}', 'us')
            print('this time it worked')
        elif which == 'forecast':
            obs = three_hours_forecast_at_coords(zlat, zlon)
            print('this time it worked')
    except APICallTimeoutError:
        time.sleep(5)
        print('caught APICallTimeoutError on second try in set_location()...trying again')
        try:
            if which ==  'weather':
                obs = owm.weather_at_zip_code(f'{code}', 'us')
                print('this time it worked')
            elif which == 'forecast':
                obs = three_hours_forecast_at_coords(zlat, zlon)
                print('this time it worked')
        except APICallTimeoutError:
            print(f'could not get past the goddamn api call for {code}! Returning with nothing but shame this time.')
            return(f'the time is {time.time()}')
