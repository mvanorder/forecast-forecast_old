''' setup the client and api and configuation for forecast-forecast '''

from urllib.parse import quote

# import forecastforecast as ff
# from ff.ETL import db_ops
from db_ops import Client
from ETL.config import OWM_API_key_masta as masta, OWM_API_key_loohoo as loohoo
from ETL.config import port, host, user, password, socket_path

# Create MongoDB client connections to local and remote databases
password = quote(password)
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
print(f'from Transform.__init__() {uri}')
client = Client(host, port)  # sets a global pymongo MongoClient object 
remote_client = Client(uri)
