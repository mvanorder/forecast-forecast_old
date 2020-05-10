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
