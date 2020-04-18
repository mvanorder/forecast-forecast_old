''' setup the client and api and configuation for forecast-forecast '''

from db_ops import Client

# all the things from the configuration files
OWM_API_key_masta = 'ec7a9ff0f4a568d9e8e6ef8b810c599e'
OWM_API_key_loohoo ='ccf670fd173f90d5ae9c84ef6372573d'
host = 'localhost'
port = 27017
user = 'chuckvanhoff'
password = 'Fe7ePrX!5L5Wh6W'
socket_path = 'cluster0-anhr9.mongodb.net/test?retryWrites=true&w=majority'

# some database stuff
client = Client(host, port)