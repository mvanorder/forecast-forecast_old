''' functions to keep handle errors from  API calls and database interacitons '''

def ssl_err(owm):
    ''' handle the APIInvalidSSLCertificateError from the pyowm module by try and try again method 

    :param owm: the pyowm connection object
    :type owm: OWM API certificate
    '''
    print(f'except first try in set_location(): APIInvalidSSLCertificateError with zipcode {code}...trying again')
    try:
        obs = owm.weather_at_zip_code(f'{code}', 'us')
        print('this time it worked')
    except APIInvalidSSLCertificateError:
        print('except on second try in set_location(): APIInvalidSSLCertificateError - reestablishing the OWM object and trying again.')
        try:
            owm = OWM(API_key)    # the OWM object
            obs = owm.weather_at_zip_code(f'{code}', 'us')
            print('this time it worked')
        except APIInvalidSSLCertificateError:
            print('....and again... this time I am just gonna return.')
            return(f'the time is {time.time()}')

def timeout_err(owm):
    ''' handle the APIInvalidSSLCertificateError from the pyowm module by try and try again method 

    :param owm: the pyowm connection object
    :type owm: OWM API certificate
    '''
    print('caught APICallTimeoutError on first try in set_location()...trying again')
    time.sleep(5)
    try:
        obs = owm.weather_at_zip_code(f'{code}', 'us')
        print('this time it worked')
    except APICallTimeoutError:
        time.sleep(5)
        print('caught APICallTimeoutError on second try in set_location()...trying again')
        try:
            obs = owm.weather_at_zip_code(f'{code}', 'us')
            print('this time it worked')
        except APICallTimeoutError:
            print(f'could not get past the goddamn api call for {code}! Returning with nothing but shame this time.')
            return(f'the time is {time.time()}')
