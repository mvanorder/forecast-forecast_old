''' Setup the client and api and configuation for forecast-forecast. '''

from urllib.parse import quote

from config import database
import forecastforecast as ff
import ff.ETL
from ff.ETL import db_ops
from db_ops import Client
from ETL.config import OWM_API_key_masta as masta, OWM_API_key_loohoo as loohoo
from ETL.config import port, host, user, password, socket_path

# These will be used for OWM and MongoDB requests and connections.
OWM_API_key_masta = 'ec7a9ff0f4a568d9e8e6ef8b810c599e'
OWM_API_key_loohoo ='ccf670fd173f90d5ae9c84ef6372573d'
host = 'localhost'
port = 27017
user = 'chuckvanhoff'
password = 'Fe7ePrX!5L5Wh6W'
socket_path = 'cluster0-anhr9.mongodb.net/'  #test?retryWrites=true&w=majority'
password = quote(password)
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
print(uri)

# Setup some database stuff.
client = Client(host, port)  # sets a global pymongo MongoClient object 
remote_client = Client(uri)
