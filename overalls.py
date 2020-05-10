''' general use functions and classes '''

def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
    :param filename: the name of the file
    :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')
    
def key_list(d=dict):
    ''' Write the dict keys to a list and return that list 
    
    :param d: A python dictionary
    :return: A list fo the keys from the python dictionary
    '''
    
    keys = d.keys()
    key_list = []
    for k in keys:
        key_list.append(k)
    return key_list

def all_keys(d):
    ''' Get all the dicitonary and nested "dot format" nested dictionary keys
    from a dict, add them to a list, return the list.
    
    :param d: A python dictionary
    :return: A list of every key in the dictionary
    '''
    keys = []    
    for key, value in d.items():
        if isinstance(d[key], dict):
            for sub_key in all_keys(value):
                keys.append(f'{key}.{sub_key}')
        else:
            keys.append(str(key))
    return keys