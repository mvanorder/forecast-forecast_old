from urllib.parse import quote

import forecastforecast as ff
# from pymongo import MongoClient

from ff.ETL import db_ops# import Client
from ETL.config import OWM_API_key_masta as masta, OWM_API_key_loohoo as loohoo
from ETL.config import port, host, user, password, socket_path

# create a local and a remote client
password = quote(password)    # url encode the password for the mongodb uri
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
print(uri)
client = Client(host, port)
remote_client = Client(uri)
